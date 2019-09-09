//! Crash Recovery Log
//! 
//! This module implements a durable log for transaction and object allocation state to ensure
//! that those operations can be successfully recovered in the event of unexpected program
//! termination. 
//! 
//! The CRL is implemented in front-end, backend-halves where the frontend is common to all
//! CRL implementations and the back end is abstracted away behind a std::async::mpsc::Sender
//! interface.

use super::{ ArcData, ArcDataSlice };
use super::hlc;
use super::object;
use super::paxos;
use super::store;
use super::transaction;

pub mod sweeper;

#[derive(Debug)]
struct DecodeError;

impl From<crate::EncodingError> for DecodeError {
    fn from(_: crate::EncodingError) -> DecodeError {
        DecodeError{}
    }
}

/// Unique Identifier for a state save request made to the Crash Recovery Log
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, Hash)]
pub struct RequestId(u64);

/// Used to notify completion of state save requests made to the Crash Recovery Log
/// 
/// Methods on the handler are called within the context of a CRL thread. Consequently,
/// implementations of this trait should wrap a std::sync::mpsc::Sender and use it
/// to send the completion notice back to the thread that originated the request. A
/// trait object is used rather than doing this directly to allow for flexibility in
/// the message type sent over the channel
pub trait SaveCompleteHandler {
    fn transaction_state_saved(&self, request_id: RequestId);
    fn allocation_state_saved(&self, request_id: RequestId);
}

/// Factory interface for Crl creation
pub trait InterfaceFactory {
    /// Creates a new Crl trait object that will notify the supplied SaveCompleteHandler
    /// when requests complete
    fn new(&self, save_handler: Box<dyn SaveCompleteHandler>) -> Box<dyn Crl>;
}

/// Represents the persistent state needed to recover a transaction after a crash
#[derive(Clone, Eq, PartialEq)]
pub struct TransactionRecoveryState {
    store_id: store::Id,
    serialized_transaction_description: ArcData,
    object_updates: Vec<transaction::ObjectUpdate>,
    tx_disposition: transaction::Disposition,
    paxos_state: paxos::PersistentState
}

/// Represents the persistent state needed to recover an allocation operation after a crash
#[derive(Clone, Eq, PartialEq)]
pub struct AllocationRecoveryState {
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
        serialized_transaction_description: Option<ArcData>,
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