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

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use crate::{ArcData, ArcDataSlice, Data, DataMut, DataReader};
use crate::hlc;
use crate::object;
use crate::paxos;
use crate::store;
use crate::transaction;
use super::{TransactionRecoveryState, AllocationRecoveryState};
use super::DecodeError;

pub mod stream;
pub(crate) mod buffer_mgr;
pub(crate) mod encoding;

pub use self::stream::Stream;

/// store::Id + UUID
const TXID_SIZE: u64 = 17 + 16;

// 2 byte file id + 8 byte offset + 4 byte length
const FILE_LOCATION_SIZE: u64 = 2 + 8 + 4;

// Static Transaction Block Size {
//     store_id: store::Id, 17
//     transaction_id: 16
//     serialized_transaction_description: Bytes, 14 (FileLocation)
//     tx_disposition: transaction::Disposition, 1
//     paxos_state: paxos::PersistentState, 11 (1:mask-byte + 5:proposalId + 5:proposalId)
//     object_updates: Vec<transaction::ObjectUpdate>, 4:count (trailing data is num_updates * (16:objuuid + FileLocation))
// }
const STATIC_TX_SIZE: u64 = TXID_SIZE + FILE_LOCATION_SIZE + 1 + 11 + 4;

/// Entry block always ends with:
///   entry_serial_number - 8
///   entry_begin_offset - 8
///   earliest_entry_needed - 8
///   num_transactions - 4
///   num_allocations - 4
///   num_tx_deletions - 4
///   num_alloc_deletions - 4
///   prev_entry_file_location - 14 (2 + 8 + 4)
///   file_uuid - 16
const STATIC_ENTRY_SIZE: u64 = 8 + 8 + 8 + 4 + 4 + 4 + 4 + 14 + 16;

//
// pub struct AllocationRecoveryState {
//     store_id: store::Id, 17
//     allocation_transaction_id: transaction::Id, 16
//     store_pointer: object::StorePointer,  <== 4 + nbytes
//     id: object::Id, 16
//     kind: object::Kind, 1
//     size: Option<u32>, 4 - 0 means None
//     data: Bytes, 14 = FileLocation
//     refcount: object::Refcount, 8
//     timestamp: hlc::Timestamp, 8
//     serialized_revision_guard: Bytes <== 4 + nbytes
// }
const STATIC_ARS_SIZE: u64 = 17 + 16 + 4 + 16 + 1 + 4 + 14 + 8 + 8 + 4;

/// Combines a store::Id and transaction::Id into a single identifier.
/// 
/// Both items must be used in conjunction to uniquely identify transaction/allocation state
/// since multiple stores on the same host may be part of the same transaction.
#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
pub(crate) struct TxId(store::Id, transaction::Id);

/// Wrapper for numericly identified file
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, Hash)]
pub struct FileId(u16);

/// Identifies teh file, offset, and length of data stored in a file
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct FileLocation {
    file_id: FileId,
    offset: u64,
    length: u32
}

impl TxId {
    fn encode_into(&self, buf: &mut DataMut) {
        buf.put_uuid(self.0.pool_uuid);
        buf.put_u8(self.0.pool_index);
        buf.put_uuid((self.1).0);
    }

    fn decode_from(buf: &mut Data) -> TxId {
        let pool_uuid = buf.get_uuid();
        let pool_index = buf.get_u8();
        let transaction_id = transaction::Id(buf.get_uuid());
        TxId(store::Id {pool_uuid, pool_index }, transaction_id)
    }
}

impl FileLocation {
    fn encode_into(&self, buf: &mut DataMut) {
        buf.put_u16_le(self.file_id.0);
        buf.put_u64_le(self.offset);
        buf.put_u32_le(self.length);
    }

    fn null() -> FileLocation {
        FileLocation {
            file_id: FileId(0),
            offset: 0,
            length: 0
        }
    }
    
    fn decode_from(buf: &mut Data) -> FileLocation {
        let file_id = buf.get_u16_le();
        let offset = buf.get_u64_le();
        let length = buf.get_u32_le();
        FileLocation {
            file_id : FileId(file_id),
            offset,
            length
        }
    }
}


pub(crate) struct Tx {
    id: transaction::Id,
    txd_location: Option<FileLocation>,
    data_locations: Option<Vec<FileLocation>>,
    state: TransactionRecoveryState,
    last_entry_serial: u64
}

#[derive(Eq, PartialEq, Debug)]
pub(crate) struct RecoveringTx {
    id: TxId,
    serialized_transaction_description: FileLocation,
    object_updates: Vec<(uuid::Uuid, FileLocation)>,
    tx_disposition: transaction::Disposition,
    paxos_state: paxos::PersistentState,
    last_entry_serial: u64
}

pub struct RecoveredTx {
    id: TxId,
    txd_location: FileLocation,
    serialized_transaction_description: ArcData,
    object_updates: Vec<transaction::ObjectUpdate>,
    update_locations: Vec<(uuid::Uuid, FileLocation)>,
    tx_disposition: transaction::Disposition,
    paxos_state: paxos::PersistentState,
    last_entry_serial: u64
}

pub(crate) struct Alloc {
    data_location: Option<FileLocation>,
    state: AllocationRecoveryState,
    last_entry_serial: u64
}

#[derive(Eq, PartialEq, Debug)]
pub(crate) struct RecoveringAlloc {
    id: TxId,
    store_pointer: store::Pointer,
    object_id: object::Id,
    kind: object::Kind,
    size: Option<u32>,
    data: FileLocation,
    refcount: object::Refcount,
    timestamp: hlc::Timestamp,
    serialized_revision_guard: ArcDataSlice,
    last_entry_serial: u64
}

pub struct RecoveredAlloc {
    id: TxId,
    store_pointer: store::Pointer,
    object_id: object::Id,
    kind: object::Kind,
    size: Option<u32>,
    data_location: FileLocation,
    data: ArcData,
    refcount: object::Refcount,
    timestamp: hlc::Timestamp,
    serialized_revision_guard: ArcDataSlice,
    last_entry_serial: u64
}

pub(crate) struct EntryBuffer {
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

    fn is_empty(&self) -> bool {
        self.requests.is_empty() && self.tx_set.is_empty() &&
        self.tx_deletions.is_empty() && self.alloc.is_empty() &&
        self.alloc_deletions.is_empty()
    }

    fn add_transaction(&mut self, tx_id: TxId) {
        self.tx_set.insert(tx_id);
    }

    fn add_allocation(&mut self, tx_id: TxId) {
        self.alloc.push(tx_id);
    }

    fn drop_transaction(&mut self, tx_id: TxId) {
        self.tx_deletions.push(tx_id);
    }

    fn drop_allocation(&mut self, tx_id: TxId) {
        self.alloc_deletions.push(tx_id);
    }
}

pub struct Sweeper<T: Stream> {
    streams: Vec<T>,
    buf_mgr: buffer_mgr::BufferManager
}

impl<T: Stream> Sweeper<T> {

    pub fn new(
        streams: Vec<T>,
        entry_window_size: usize,
        recovered_transactions: &Vec<RecoveredTx>,
        recovered_allocations: &Vec<RecoveredAlloc>) -> Sweeper<T> {

        let buf_mgr = buffer_mgr::BufferManager::new(&streams, entry_window_size, 
            recovered_transactions, recovered_allocations);

        Sweeper {
            streams: streams,
            buf_mgr: buf_mgr
        }
    }

    fn find_ready_stream(&self) -> Option<usize> {
        for (idx, s) in self.streams.iter().enumerate() {
            if s.is_ready() {
                return Some(idx);
            }
        }
        None
    }

    pub fn commit_entry(&mut self) {
        if ! self.buf_mgr.is_empty() {
            if let Some(idx) = self.find_ready_stream() {
                self.buf_mgr.log_entry(&self.streams[idx]);
            }
        }
    }
}



