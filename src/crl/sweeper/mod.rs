//! This module implements a Crash Recovery Log backend by way of multiple files
//! with interleaved log entries. 
//! 
//! Multiple files are used to reduce latency by allowing us to write new journal
//! entries before the previous ones have finished syncing to disk. Log compaction
//! is accomplished by periodically switching over to new log files and copying over
//! only those transactions that haven't finished executing.
//! 
//! txd and data are written first, locations updated in Tx/Alloc structs
//! On Tx update, always write full Tx state. keeps it easier. Tx entry
//! size = 16+17+1+8+4+1+1+8+ (8+4)*num_updates ~70 bytes.
//! 
//! Full active Tx and Alloc snapshot every XX number updates?
//! 3 File solution for each stream.: 
//!   Write to 0 file till it hits YY Mb
//!   Write to 1 till it hits YY Mb
//!   Copy all data stored in file 0 to file 2. Truncate 0, rotate files
//! 
//! 
//! 
//! 





use std::mem;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use bytes::{Bytes, BytesMut, Buf, BufMut};

use crate::paxos;
use crate::store;
use crate::transaction;
use super::{TransactionRecoveryState, AllocationRecoveryState};

#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
struct TxId(store::Id, transaction::Id);

impl TxId {
    fn encode_into<T: BufMut>(&self, buf: &mut T) {
        buf.put_slice(self.0.pool_uuid.as_bytes());
        buf.put_u8(self.0.pool_index);
        buf.put_slice((self.1).0.as_bytes());
    }
}

const TXID_SIZE: u64 = 17 + 16;

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, Hash)]
pub struct FileId(u16);

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, Hash)]
pub struct StreamId(u16);

struct FileLocation {
    file_id: FileId,
    offset: u64,
    length: u32
}

impl FileLocation {
    fn encode_into<T: BufMut>(&self, buf: &mut T) {
        buf.put_u16_le(self.file_id.0);
        buf.put_u64_le(self.offset);
        buf.put_u32_le(self.length);
    }
}

const FILE_LOCATION_SIZE: u64 = 2 + 8 + 4;

struct Tx {
    id: transaction::Id,
    txd_location: Option<FileLocation>,
    data_locations: Option<Vec<FileLocation>>,
    state: TransactionRecoveryState
}

struct Alloc {
    data_location: Option<FileLocation>,
    state: AllocationRecoveryState
}

struct Request {
    client_id: super::ClientId,
    request_id: super::RequestId
}

struct EntryBuffer {
    requests: Vec<super::RequestId>,
    tx_set: HashSet<TxId>,
    tx_deletions: Vec<TxId>,
    alloc: Vec<TxId>,
    alloc_deletions: Vec<TxId>
}

impl EntryBuffer {
    fn new() -> EntryBuffer {
        EntryBuffer {
            requests: Vec::new(),
            tx_set: HashSet::new(),
            tx_deletions: Vec::new(),
            alloc: Vec::new(),
            alloc_deletions: Vec::new()
        }
    }

    fn clear(&mut self) {
        self.requests.clear();
        self.tx_set.clear();
        self.tx_deletions.clear();
        self.alloc.clear();
        self.alloc_deletions.clear();
    }

    fn add_transaction(&mut self, tx_id: TxId) {
        self.tx_set.insert(tx_id);
    }

    fn add_allocation(&mut self, tx_id: TxId) {
        self.alloc.push(tx_id);
    }
}

pub trait Stream {

    fn id(&self) -> StreamId;

    /// Returns the current file Id, Offset, & Maximum File Size
    fn status(&self) -> (FileId, u64, u64);

    /// Writes the provided data to the file provided by status()
    fn write(&mut self, data: Vec<Bytes>);

    /// Rotates the underlying files and optionally returns a FileId
    /// to prune entries from
    fn rotate_files(&mut self) -> Option<FileId>;
}

pub struct Sweeper<T: Stream> {
    transactions: HashMap<TxId, RefCell<Tx>>,
    allocations: HashMap<TxId, RefCell<Alloc>>,
    buffers: Vec<RefCell<EntryBuffer>>,
    current_buffer: RefCell<EntryBuffer>,
    streams: Vec<T>,
    entry_serial: u64
}

impl<T: Stream> Sweeper<T> {

    pub fn new() -> Sweeper<T> {
        Sweeper {
            transactions: HashMap::new(),
            allocations: HashMap::new(),
            buffers: Vec::new(),
            current_buffer: RefCell::new(EntryBuffer::new()),
            streams: Vec::new(),
            entry_serial: 0
        }
    }

    fn finalize_entry(&mut self) -> RefCell<EntryBuffer> {
        mem::replace(&mut self.current_buffer,
            self.buffers.pop().unwrap_or_else(|| RefCell::new(EntryBuffer::new())))
    }

    fn recycle_buffer(&mut self, buf: RefCell<EntryBuffer>) {
        buf.borrow_mut().clear();
        self.buffers.push(buf);
    }

    fn prune_data_stored_in_file(&self, file_id: FileId) {
        let mut current_buffer = self.current_buffer.borrow_mut();

        for (tx_id, tx) in &self.transactions {
            let mut mtx = tx.borrow_mut();

            if let Some(l) = mtx.txd_location.as_ref() {
                if l.file_id == file_id {
                    mtx.txd_location = None;
                    current_buffer.add_transaction(*tx_id);
                }

                if let Some(data_locations) = mtx.data_locations.as_ref() {
                    for l in data_locations {
                        if l.file_id == file_id {
                            mtx.data_locations = None;
                            current_buffer.add_transaction(*tx_id);
                            break;
                        }
                    }
                }
            }
        }

        for (tx_id, a) in &self.allocations {
            let mut ma = a.borrow_mut();

            if let Some(l) = ma.data_location.as_ref() {
                if l.file_id == file_id {
                    ma.data_location = None;
                    current_buffer.add_allocation(*tx_id);
                }
            }
        }
    }

    pub fn log_entry(&mut self, stream: &mut T) {

        let buf = self.finalize_entry();

        let mut txs: Vec<&RefCell<Tx>> = Vec::new();
        let mut allocs: Vec<&RefCell<Alloc>> = Vec::new();

        for txid in &buf.borrow().tx_set {
            self.transactions.get(&txid).map( |tx| txs.push(tx) );
        }

        for txid in &buf.borrow().alloc {
            self.allocations.get(&txid).map( |a| allocs.push(a) );
        }

        let (mut file_id, mut offset, mut max_size) = stream.status();

        let (data_sz, padding_sz, tail_sz, num_data_buffers) = calculate_write_size(offset, 
            &txs, &allocs, &buf.borrow().tx_deletions, &buf.borrow().alloc_deletions);

        if offset + data_sz + padding_sz + tail_sz > max_size {
            if let Some(prune_file_id) = stream.rotate_files() {
                // We've already finalized the buffer for the current log entry. This method will
                // find all tx/allocs that have data stored in the file and enter them into the
                // next buffer
                self.prune_data_stored_in_file(prune_file_id);
            }
            let (file_id, offset, max_size) = stream.status();
        }

        let mut tail = BytesMut::with_capacity((padding_sz + tail_sz) as usize);

        zfill(padding_sz, &mut tail);

        let mut buffers = Vec::<Bytes>::with_capacity(num_data_buffers + 1);

        let mut push_data_buffer = |b: &Bytes| -> FileLocation {
            let length = b.len();
            let l = FileLocation{file_id : file_id, offset : offset, length : length as u32};
            buffers.push(b.clone());
            offset += length as u64;
            l  
        };

        for tx in txs {
            let mut mtx = tx.borrow_mut();

            if mtx.txd_location.is_none() {
                mtx.txd_location = Some(push_data_buffer(&mtx.state.serialized_transaction_description));
            }

            if mtx.data_locations.is_none() && !mtx.state.object_updates.is_empty() {
                let mut v = Vec::new();
                for ou in &mtx.state.object_updates {
                    v.push(push_data_buffer(&ou.1));
                }
                mtx.data_locations = Some(v);
            }

            drop(mtx);

            encode_tx_state(&tx.borrow(), &mut tail);   
        }

        for a in allocs.iter_mut() {
            let mut ma = a.borrow_mut();

            if ma.data_location.is_none() {
                ma.data_location = Some(push_data_buffer(&ma.state.data));
            }

            drop(ma);

            encode_alloc_state(&a.borrow(), &mut tail);
        }

        for id in &buf.borrow().tx_deletions {
            id.encode_into(&mut tail);   
        }

        for id in &buf.borrow().alloc_deletions {
            id.encode_into(&mut tail);
        }
    }
}

fn zfill<T: BufMut>(nbytes: u64, buf: &mut T) {
    let mut remaining = nbytes;

    while remaining > 8 {
        buf.put_u64_le(0u64);
        remaining -= 8;
    }

    while remaining > 0 {
        buf.put_u8(0u8);
        remaining -= 1;
    }
}

// pub struct TransactionRecoveryState {
//     store_id: store::Id, 17
//     transaction_id: 16
//     serialized_transaction_description: Bytes, 14 (FileLocation)
//     object_updates: Vec<transaction::ObjectUpdate>, 4:count + num_updates * (16:objuuid + FileLocation)
//     tx_disposition: transaction::Disposition, 1
//     paxos_state: paxos::PersistentState, 11 (1:mask-byte + 5:proposalId + 5:proposalId)
// }
const STATIC_TX_SIZE: u64 = 17 + 16 + 14 + 4 + 1 + 11;

fn encode_tx_state<T: BufMut>(tx: &Tx, buf: &mut T) {
    let tx_id = TxId(tx.state.store_id, tx.id);
    tx_id.encode_into(buf);
    
    if let Some(loc) = &tx.txd_location {
        loc.encode_into(buf);
    }

    match &tx.data_locations {
        None => buf.put_u8(0u8),

        Some(dl) => {
            buf.put_u8(dl.len() as u8);
            for (ou, loc) in tx.state.object_updates.iter().zip(dl.iter()) {
                buf.put_slice(ou.0.as_bytes());
                loc.encode_into(buf);
            }
        }
    }
    
    buf.put_u8(tx.state.tx_disposition.to_u8());
    let mut mask = 0u8;
    let mut promise_peer = 0u8;
    let mut promise_proposal_id = 0u32;
    let mut accepted_peer = 0u8;
    let mut accepted_proposal_id = 0u32;
    if let Some(promise) = tx.state.paxos_state.promised {
        mask |= 1 << 2;
        promise_peer = promise.peer;
        promise_proposal_id = promise.number;
    }
    if let Some((prop_id, accepted)) = tx.state.paxos_state.accepted {
        mask |= 1 << 1;
        if accepted {
            mask |= 1 << 0;
        }
        accepted_peer = prop_id.peer;
        accepted_proposal_id = prop_id.number;
    }
    buf.put_u8(mask);
    buf.put_u8(promise_peer);
    buf.put_u32_le(promise_proposal_id);
    buf.put_u8(accepted_peer);
    buf.put_u32_le(accepted_proposal_id);
}

//
// pub struct AllocationRecoveryState {
//     store_id: store::Id, 17
//     allocation_transaction_id: transaction::Id, 16
//     store_pointer: object::StorePointer,  <== 4 + nbytes
//     id: object::Id, 16
//     kind: object::Kind, 1
//     size: Option<u32>, 4 - 0 means None
//     data: Bytes, 14 = 2 + 8 + 4
//     refcount: object::Refcount, 8
//     timestamp: hlc::Timestamp, 8
//     serialized_revision_guard: Bytes <== 4 + nbytes
// }
const STATIC_ARS_SIZE: u64 = 17 + 16 + 4 + 16 + 1 + 4 + 14 + 8 + 8 + 4;

fn encode_alloc_state<T: BufMut>(a: &Alloc, buf: &mut T) {
    let tx_id = TxId(a.state.store_id, a.state.allocation_transaction_id);
    tx_id.encode_into(buf);

    buf.put_u32_le(a.state.store_pointer.0.len() as u32);
    buf.put_slice(&a.state.store_pointer.0);
    buf.put_slice(a.state.id.0.as_bytes());
    buf.put_u8(a.state.kind.to_u8());
    buf.put_u32_le(match a.state.size {
        None => 0u32,
        Some(len) => len
    });
    match &a.data_location {
        None => {
            buf.put_u16_le(0);
            buf.put_u64_le(0);
            buf.put_u32_le(0);
        }
        Some(loc) => loc.encode_into(buf)
    }
    buf.put_u32_le(a.state.refcount.update_serial);
    buf.put_u32_le(a.state.refcount.count);
    buf.put_u64_le(a.state.timestamp.to_u64());
    buf.put_u32_le(a.state.serialized_revision_guard.len() as u32);
    buf.put_slice(&a.state.serialized_revision_guard);
}

/// Calculates the size required for the write.
/// 
/// Entry block always ends with:
///   entry_serial_number - 8
///   entry_begin_offset - 8
///   entry_size - 4
///   num_transactions - 4
///   num_allocations - 4
///   num_tx_deletions - 4
///   num_alloc_deletions - 4
///   prev_entry_file_location - 14 (2 + 8 + 4)
///   entry_hash - 16
///   file_uuid - 16
/// 
/// Returns (size-of-pre-entry-data, 4k-alignment-padding-bytes, size-of-entry-block, number-of-data-buffers)
fn calculate_write_size(
    offset: u64, 
    txs: &Vec<&RefCell<Tx>>, 
    allocs: &Vec<&RefCell<Alloc>>,
    tx_deletions: &Vec<TxId>,
    alloc_deletions: &Vec<TxId>) -> (u64, u64, u64, usize) {

    let mut update_count: u64 = 0;
    let mut buffer_count: usize = 0;
    let mut data: u64 = 0;
    let mut tail: u64 = 8 + 8 + 4 + 4 + 4 + 4 + 4 + 14 + 16 + 16; 

    for tx in txs {
        let tx = tx.borrow();

        if tx.txd_location.is_none() {
            data += tx.state.serialized_transaction_description.len() as u64;
            buffer_count += 1;
        }
        if tx.data_locations.is_none() && ! tx.state.object_updates.is_empty() {
            for ou in &tx.state.object_updates {
                data += ou.1.len() as u64;
                update_count += 1;
                buffer_count += 1;
            }
        }
    }

    // Update format is 16-byte UUID + 14-byte FileLocation
    tail += txs.len() as u64 * STATIC_TX_SIZE + update_count * (16 + FILE_LOCATION_SIZE);

    tail += allocs.len() as u64 * STATIC_ARS_SIZE;

    for a in allocs {
        let a = a.borrow();

        if a.data_location.is_none() {
            data += a.state.data.len() as u64;
            buffer_count += 1;
        }
        
        tail += a.state.store_pointer.0.len() as u64 + a.state.serialized_revision_guard.len() as u64;
    }

    tail += tx_deletions.len() as u64 * TXID_SIZE;
    tail += alloc_deletions.len() as u64 * TXID_SIZE;

    let padding = pad_to_4k_alignment(offset, data, tail);

    (data, padding, tail, buffer_count)
}

fn pad_to_4k_alignment(offset: u64, data_size: u64, tail_size: u64) -> u64 {
    let base = offset + data_size + tail_size;
    if base < 4096 {
        4096 - base
    } else {
        let remainder = base % 4096;
        if remainder == 0 {
            0
        } else {
            4096 - remainder
        }
    }
}

#[cfg(test)]
mod tests {
    
    use super::*;

    

    #[test]
    fn compare() {
        assert_eq!(pad_to_4k_alignment(0, 0, 16), 4096-16);
        assert_eq!(pad_to_4k_alignment(0, 0, 17), 4096-17);
        assert_eq!(pad_to_4k_alignment(4096, 0, 16), 4096-16);
        assert_eq!(pad_to_4k_alignment(4096, 2048, 2048), 0);
        assert_eq!(pad_to_4k_alignment(4096, 4096, 4096), 0);
        assert_eq!(pad_to_4k_alignment(0, 4096, 4096), 0);
    }
}
