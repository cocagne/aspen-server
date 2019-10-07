//! Crash Recovery Log
//! 
//! This module implements a durable log for transaction and object allocation state to ensure
//! that those operations can be successfully recovered in the event of unexpected program
//! termination. 
//! 
//! The CRL is implemented in front-end, backend-halves where the frontend is common to all
//! CRL implementations and the back end is abstracted away behind a std::async::mpsc::Sender
//! interface.

use std::error::Error;
use std::fmt;
use std::sync;

use super::{ ArcData, ArcDataSlice };
use super::hlc;
use super::object;
use super::paxos;
use super::store;
use super::transaction;

pub mod sweeper;
pub mod mock;

#[derive(Debug)]
struct DecodeError;

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CRL DecodeError")
    }
}

impl Error for DecodeError {
    fn description(&self) -> &str {
        "Invalid data encountered while decoding CRL content"
    }

    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

impl From<crate::EncodingError> for DecodeError {
    fn from(_: crate::EncodingError) -> DecodeError {
        DecodeError{}
    }
}

impl From<DecodeError> for std::io::Error {
    fn from(e: DecodeError) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    }
}

/// Unique Identifier for a state save request made to the Crash Recovery Log
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, Hash)]
pub struct RequestId(u64);

pub enum Completion {
    TransactionSave {
        store_id: store::Id,
        request_id: RequestId,
        success: bool
    },
    AllocationSave {
        store_id: store::Id,
        request_id: RequestId,
        success: bool
    },
}

/// Used to notify completion of state save requests made to the Crash Recovery Log
/// 
/// Methods on the handler are called within the context of a CRL thread. Consequently,
/// implementations of this trait should wrap a std::sync::mpsc::Sender and use it
/// to send the completion notice back to the thread that originated the request. A
/// trait object is used rather than doing this directly to allow for flexibility in
/// the message type sent over the channel. The success arguemnt will be true if the
/// state was successfully written to persistent media, false if an error occurred
pub trait RequestCompletionHandler {
    fn complete(&self, op: Completion);
}

/// Interface to the CRL backend implementation
pub trait Backend {
    fn shutdown(&mut self);
    
    /// Creates a new Crl trait object that will notify the supplied RequestCompletionHandler
    /// when requests complete
    fn new_interface(&self, 
        save_handler: sync::Arc<dyn RequestCompletionHandler + Send + Sync>) -> Box<dyn Crl>;
}

/// Represents the persistent state needed to recover a transaction after a crash
#[derive(Clone, Eq, PartialEq)]
pub struct TransactionRecoveryState {
    pub store_id: store::Id,
    pub serialized_transaction_description: ArcData,
    pub object_updates: Vec<transaction::ObjectUpdate>,
    pub tx_disposition: transaction::Disposition,
    pub paxos_state: paxos::PersistentState
}

/// Represents the persistent state needed to recover an allocation operation after a crash
#[derive(Clone, Eq, PartialEq)]
pub struct AllocationRecoveryState {
    pub store_id: store::Id,
    pub store_pointer: store::Pointer,
    pub id: object::Id,
    pub kind: object::Kind,
    pub size: Option<u32>,
    pub data: ArcDataSlice,
    pub refcount: object::Refcount,
    pub timestamp: hlc::Timestamp,
    pub allocation_transaction_id: transaction::Id,
    pub serialized_revision_guard: ArcDataSlice
}



/// Client interface to the Crash Recovery Log
pub trait Crl {

    /// Provides the full recovery state for a store
    /// 
    /// This method should only be used during data store initialization and for capturing store
    /// CRL state for transferring that store to another server.
    /// 
    /// # Panics
    /// 
    /// Panics if the channel to the CRL is closed
    fn get_full_recovery_state(
        &self, 
        store_id: store::Id) -> (Vec<TransactionRecoveryState>, Vec<AllocationRecoveryState>);

    /// Saves transaction state into the CRL
    /// 
    /// The transaction description and object updates should only be included once. When the 
    /// state has been successfully stored to persistent media, the 
    /// SaveCompleteHandler.transaction_state_saved method will be called with the RequestId 
    /// returned from this function. If an error is encountered and/or the state cannot be
    /// saved, the completion handler will never be called.
    fn save_transaction_state(
        &mut self,
        store_id: store::Id,
        transaction_id: transaction::Id,
        serialized_transaction_description: ArcData,
        object_updates: Option<Vec<transaction::ObjectUpdate>>,
        tx_disposition: transaction::Disposition,
        paxos_state: paxos::PersistentState
    ) -> RequestId;

    /// Drops transaction data from the log.
    /// 
    /// Informs the CRL that object data associated with the transaction is no longer needed
    /// for recovery purposes and that it may be dropped from the log.
    fn drop_transaction_object_data(
        &self,
        store_id: store::Id,
        transaction_id: transaction::Id
    );

    /// Deletes the saved transaction state from the log.
    fn delete_transaction_state(
        &self,
        store_id: store::Id,
        transaction_id: transaction::Id
    );

    /// Saves object allocation state into the CRL
    /// 
    /// Similar to transaction state saves, no completion notice will be provided if an error
    /// is encountered
    fn save_allocation_state(
        &mut self,
        store_id: store::Id,
        store_pointer: store::Pointer,
        id: object::Id,
        kind: object::Kind,
        size: Option<u32>,
        data: ArcDataSlice,
        refcount: object::Refcount,
        timestamp: hlc::Timestamp,
        allocation_transaction_id: transaction::Id,
        serialized_revision_guard: ArcDataSlice
    ) -> RequestId;

    fn delete_allocation_state(
        &self,
        store_id: store::Id, 
        allocation_transaction_id: transaction::Id);
}