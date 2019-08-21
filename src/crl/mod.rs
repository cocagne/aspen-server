//!
//! dyn Crl should be the public interface to the CRL (multiple concurrent impls)
//! 
//! Request/Reply enums
//! 
//! External to store, there's a RegisterReceiver(cloned-mspc-sender), RegisterResponse(CrlId)
//!     |-> Happens during store manager thread startup
//! 
//! All future Requests must include the CrlId in order to receive a response.
//! 
//! #[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, Hash)]
//! pub struct CrlTxId(storeUuid, txUuid)
//! 
//! Request {
//!    crlid: u32
//!    SaveState(<state_stuff>, CrlTxId) .. Just use hashmap rather than try to optimize with index
//!    state_save_completed(<save_result>)
//! }
//! 
//! For Tx, always save Pax state. Optionally save full Tx state
//! 

use std::sync::mpsc;

use bytes::Bytes;

use super::hlc;
use super::object;
use super::paxos;
use super::store;
use super::transaction;

/// Unique Identifier for a state save request made to the Crash Recovery Log
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
pub struct RequestId(u64);

/// Used to notify completion of state save requests made to the Crash Recovery Log
/// 
/// Implementations of this trait should wrap a std::sync::mpsc::Sender and use it
/// to send the completion notice back to the thread that originated the request. A
/// trait object is used rather than doing so directly to allow for flexibility in
/// the message type sent over the channel
pub trait SaveCompleteHandler {
    fn transaction_state_saved(&self, request_id: RequestId);
    fn allocation_state_saved(&self, request_id: RequestId);
}

/// Factory interface for Crl creation
pub trait InterfaceFactory {
    /// Creates a new Crl trait object that will notify the supplied SaveCompleteHandler
    /// when requests complete
    fn new(save_handler: Box<dyn SaveCompleteHandler>) -> Box<dyn Crl>;
}

/// Client interface to the Crash Recovery Log
pub trait Crl {

    /// Provides the full recovery state for a store
    /// 
    /// This method should only be used during data store initialization and for capturing store CRL state for
    /// transferring that store to another server.
    fn get_full_recovery_state(&self, store_id: store::Id) -> 
        (Vec<TransactionRecoveryState>, Vec<AllocationRecoveryState>);

    /// Saves transaction state into the CRL
    /// 
    /// The transaction description and object updates should only be included once. When the state
    /// has been successfully stored to persistent media, the SaveCompleteHandler.transaction_state_saved
    /// method will be called with the RequestId returned from this function. If an error is encountered
    /// and/or the state cannot be saved, the completion handler will never be called.
    fn save_transaction_state(
        &self,
        store_id: store::Id,
        transaction_id: transaction::Id,
        serialized_transaction_description: Option<Bytes>,
        object_updates: Option<Vec<transaction::ObjectUpdate>>,
        tx_disposition: transaction::Status,
        paxos_state: paxos::PersistentState
    ) -> RequestId;

    /// Deletes the saved transaction state from the log.
    fn delete_transaction_state(
        &self,
        store_id: store::Id,
        transaction_id: transaction::Id);

    /// Saves object allocation state into the CRL
    /// 
    /// Similar to transaction state saves, no completion notice will be provided if an error is encountered
    fn save_allocation_state(
        &self,
        store_id: store::Id,
        store_pointer: object::StorePointer,
        id: object::Id,
        kind: object::Kind,
        size: Option<u32>,
        data: Bytes,
        refcount: object::Refcount,
        timestamp: hlc::Timestamp,
        allocation_transaction_id: transaction::Id,
        serialized_revision_guard: Bytes
    ) -> RequestId;

    fn delete_allocation_state(
        &self,
        store_id: store::Id, 
        allocation_transaction_id: transaction::Id);
}

struct ClientId(u32);

enum Request {
    SaveTransactionState {
        client_id: ClientId,
        request_id: RequestId,
        store_id: store::Id,
        transaction_id: transaction::Id,
        serialized_transaction_description: Option<Bytes>,
        object_updates: Option<Vec<transaction::ObjectUpdate>>,
        tx_disposition: transaction::Status,
        paxos_state: paxos::PersistentState
    },
    DeleteTransactionState {
        store_id: store::Id,
        transaction_id: transaction::Id,
    },
    SaveAllocationState {
        client_id: ClientId,
        request_id: RequestId,
        state: AllocationRecoveryState
    },
    DeleteAllocationState {
        store_id: store::Id,
        allocation_transaction_id: transaction::Id,
    }
}

pub enum Response {
    TransactionStateSaved(RequestId),
    AllocationStateSaved(RequestId)
}

/// Represents the persistent state needed to recover a transaction after a crash
pub struct TransactionRecoveryState {
    store_id: store::Id,
    serialized_transaction_description: Bytes,
    object_updates: Vec<transaction::ObjectUpdate>,
    tx_disposition: transaction::Status,
    paxos_state: paxos::PersistentState
}

/// Represents the persistent state needed to recover an allocation operation after a crash
pub struct AllocationRecoveryState {
    store_id: store::Id,
    store_pointer: object::StorePointer,
    id: object::Id,
    kind: object::Kind,
    size: Option<u32>,
    data: Bytes,
    refcount: object::Refcount,
    timestamp: hlc::Timestamp,
    allocation_transaction_id: transaction::Id,
    serialized_revision_guard: Bytes
}
