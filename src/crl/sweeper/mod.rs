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

// pub struct TransactionRecoveryState {
//     store_id: store::Id, 17
//     serialized_transaction_description: Bytes, 14 (FileLocation)
//     object_updates: Vec<transaction::ObjectUpdate>, 4 + num_updates * FileLocation
//     tx_disposition: transaction::Disposition, 1
//     paxos_state: paxos::PersistentState, 11 (1:mask-byte + 5:proposalId + 5:proposalId)
// }
const STATIC_TX_SIZE: u64 = 17 + 14 + 4 + 1 + 11;
//
// pub struct AllocationRecoveryState {
//     store_id: store::Id, 17
//     store_pointer: object::StorePointer,  <== 4 + nbytes
//     id: object::Id, 16
//     kind: object::Kind, 1
//     size: Option<u32>, 4 - 0 means None
//     data: Bytes, 14 = 2 + 8 + 4
//     refcount: object::Refcount, 8
//     timestamp: hlc::Timestamp, 8
//     allocation_transaction_id: transaction::Id, 16
//     serialized_revision_guard: Bytes <== 4 + nbytes
// }
const STATIC_ARS_SIZE: u64 = 17 + 4 + 16 + 1 + 4 + 14 + 8 + 8 + 16 + 4;

use std::mem;
use std::collections::{HashMap, HashSet};

use bytes::{Bytes, BytesMut, BufMut};

use crate::paxos;
use crate::store;
use crate::transaction;
use super::{TransactionRecoveryState, AllocationRecoveryState};

#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
struct TxId(store::Id, transaction::Id);

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, Hash)]
pub struct FileId(u16);

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, Hash)]
pub struct StreamId(u16);

struct FileLocation {
    file_id: FileId,
    offset: u64,
    length: u32
}

const FILE_LOCATION_SIZE: u64 = 2 + 8 + 4;

struct Tx {
    txd_location: Option<FileLocation>,
    data_locations: Vec<FileLocation>,
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

    fn add_transaction(&mut self, store_id: store::Id, transaction_id: transaction::Id) {
        self.tx_set.insert(TxId(store_id, transaction_id));
    }

    fn add_allocation(&mut self, store_id: store::Id, transaction_id: transaction::Id) {
        self.alloc.push(TxId(store_id, transaction_id));
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
    transactions: HashMap<TxId, Tx>,
    allocations: HashMap<TxId, Alloc>,
    buffers: Vec<Box<EntryBuffer>>,
    current_buffer: Box<EntryBuffer>,
    streams: Vec<T>,
    entry_serial: u64
}

impl<T: Stream> Sweeper<T> {

    pub fn new() -> Sweeper<T> {
        Sweeper {
            transactions: HashMap::new(),
            allocations: HashMap::new(),
            buffers: Vec::new(),
            current_buffer: Box::new(EntryBuffer::new()),
            streams: Vec::new(),
            entry_serial: 0
        }
    }

    fn finalize_entry(&mut self) -> Box<EntryBuffer> {
        mem::replace(&mut self.current_buffer,
            self.buffers.pop().unwrap_or_else(|| Box::new(EntryBuffer::new())))
    }

    fn recycle_buffer(&mut self, mut buf: Box<EntryBuffer>) {
        buf.clear();
        self.buffers.push(buf);
    }

    fn prune_data_stored_in_file(&mut self, file_id: FileId) {

    }

    fn log_entry(&mut self, stream: &mut T) {

        let buf = self.finalize_entry();

        let mut txs: Vec<&Tx> = Vec::new();
        let mut allocs: Vec<&Alloc> = Vec::new();

        for txid in buf.tx_set {
            self.transactions.get(&txid).map( |tx| txs.push(tx) );
        }

        for txid in buf.alloc {
            self.allocations.get(&txid).map( |a| allocs.push(a) );
        }

        let (mut file_id, mut offset, mut max_size) = stream.status();

        let (data_sz, padding_sz, tail_sz, num_data_buffers) = calculate_write_size(offset, &txs, &allocs);

        if offset + data_sz + padding_sz + tail_sz > max_size {
            if let Some(prune_file_id) = stream.rotate_files() {
                self.prune_data_stored_in_file(prune_file_id);
            }
            let (file_id, offset, max_size) = stream.status();
        }

        let mut tail = BytesMut::with_capacity((padding_sz + tail_sz) as usize);

        zfill(padding_sz, &mut tail);

        let buffers = Vec::<Bytes>::with_capacity(num_data_buffers + 1);

        // Define tail block layout
        //   AND update static tail size
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


/// Calculates the size required for the write.
/// 
/// Entry block always ends with:
///   entry_serial_number - 8
///   entry_begin_offset - 8
///   entry_size - 4
///   prev_entry_file_location - 14 (2 + 8 + 4)
///   entry_hash - 16
///   file_uuid - 16
/// 
/// Returns (size-of-pre-entry-data, 4k-alignment-padding-bytes, size-of-entry-block, number-of-data-buffers)
fn calculate_write_size(offset: u64, txs: &Vec<&Tx>, allocs: &Vec<&Alloc>) -> (u64, u64, u64, usize) {
    let mut update_count: u64 = 0;
    let mut buffer_count: usize = 0;
    let mut data: u64 = 0;
    let mut tail: u64 = 8 + 8 + 4 + 14 + 16 + 16; 

    for tx in txs {
        if tx.txd_location.is_none() {
            data += tx.state.serialized_transaction_description.len() as u64;
            buffer_count += 1;
        }
        if tx.data_locations.is_empty() && ! tx.state.object_updates.is_empty() {
            for ou in &tx.state.object_updates {
                data += ou.len() as u64;
                update_count += 1;
                buffer_count += 1;
            }
        }
    }

    tail += txs.len() as u64 * STATIC_TX_SIZE + update_count * FILE_LOCATION_SIZE;

    tail += allocs.len() as u64 * STATIC_ARS_SIZE;

    for a in allocs {
        if a.data_location.is_none() {
            data += a.state.data.len() as u64;
            buffer_count += 1;
        }
        
        tail += a.state.store_pointer.0.len() as u64 + a.state.serialized_revision_guard.len() as u64;
    }

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
