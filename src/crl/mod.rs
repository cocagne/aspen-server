//! Crash Recovery Log
//! 
//! This module implements a durable log for transaction and object allocation state to ensure
//! that those operations can be successfully recovered in the event of unexpected program
//! termination. 
//! 
//! The CRL is implemented in front-end, backend-halves where the frontend is common to all
//! CRL implementations and the back end is abstracted away behind a std::async::mpsc::Sender
//! interface.

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
    fn new(save_handler: Box<dyn SaveCompleteHandler>) -> Crl;
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

/// Client interface to the Crash Recovery Log
pub struct Crl {
    client_id: ClientId,
    sender: std::sync::mpsc::Sender<Request>,
    next_request_number: u64
}

impl Crl {

    fn new(client_id: ClientId, sender: std::sync::mpsc::Sender<Request>) -> Crl {
        Crl { 
            client_id: client_id,
            sender: sender,
            next_request_number: 0
        }
    }

    /// Provides the full recovery state for a store
    /// 
    /// This method should only be used during data store initialization and for capturing store CRL state for
    /// transferring that store to another server.
    /// 
    /// # Panics
    /// 
    /// Panics if the channel to the CRL is closed
    pub fn get_full_recovery_state(&self, store_id: store::Id) -> 
        (Vec<TransactionRecoveryState>, Vec<AllocationRecoveryState>) {
        let (response_sender, receiver) = std::sync::mpsc::channel();
        self.sender.send(Request::GetFullRecoveryState{store_id: store_id, sender: response_sender}).unwrap();
        let t = receiver.recv().unwrap();
        (t.0, t.1)
    }

    fn next_request(&mut self) -> RequestId {
        let request_id = RequestId(self.next_request_number);
        self.next_request_number += 1;
        request_id
    }

    /// Saves transaction state into the CRL
    /// 
    /// The transaction description and object updates should only be included once. When the state
    /// has been successfully stored to persistent media, the SaveCompleteHandler.transaction_state_saved
    /// method will be called with the RequestId returned from this function. If an error is encountered
    /// and/or the state cannot be saved, the completion handler will never be called.
    pub fn save_transaction_state(
        &mut self,
        store_id: store::Id,
        transaction_id: transaction::Id,
        serialized_transaction_description: Option<Bytes>,
        object_updates: Option<Vec<transaction::ObjectUpdate>>,
        tx_disposition: transaction::Disposition,
        paxos_state: paxos::PersistentState
    ) -> RequestId {
        let request_id = self.next_request();
        self.sender.send(Request::SaveTransactionState{
            client_id: self.client_id,
            request_id: request_id,
            store_id: store_id,
            transaction_id: transaction_id,
            serialized_transaction_description: serialized_transaction_description,
            object_updates: object_updates,
            tx_disposition: tx_disposition,
            paxos_state: paxos_state
        }).unwrap_or(()); // Explicitly ignore any errors
        request_id
    }

    /// Deletes the saved transaction state from the log.
    pub fn delete_transaction_state(
        &self,
        store_id: store::Id,
        transaction_id: transaction::Id
    ) {
        self.sender.send(Request::DeleteTransactionState{
            store_id: store_id,
            transaction_id: transaction_id,
        }).unwrap_or(()); // Explicitly ignore any errors
    }

    /// Saves object allocation state into the CRL
    /// 
    /// Similar to transaction state saves, no completion notice will be provided if an error is encountered
    pub fn save_allocation_state(
        &mut self,
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
    ) -> RequestId {
        let request_id = self.next_request();
        self.sender.send(Request::SaveAllocationState{
            client_id: self.client_id,
            request_id: request_id,
            state: AllocationRecoveryState {
                store_id: store_id,
                store_pointer: store_pointer,
                id: id,
                kind: kind,
                size: size,
                data: data,
                refcount: refcount,
                timestamp: timestamp,
                allocation_transaction_id: allocation_transaction_id,
                serialized_revision_guard: serialized_revision_guard
            }
        }).unwrap_or(()); // Explicitly ignore any errors
        request_id
    }

    pub fn delete_allocation_state(
        &self,
        store_id: store::Id, 
        allocation_transaction_id: transaction::Id) {
        self.sender.send(Request::DeleteAllocationState{
            store_id: store_id,
            allocation_transaction_id: allocation_transaction_id
        }).unwrap_or(()); // Explicitly ignore any errors
    }
}

#[derive(Clone, Copy)]
struct ClientId(u32);

enum Request {
    SaveTransactionState {
        client_id: ClientId,
        request_id: RequestId,
        store_id: store::Id,
        transaction_id: transaction::Id,
        serialized_transaction_description: Option<Bytes>,
        object_updates: Option<Vec<transaction::ObjectUpdate>>,
        tx_disposition: transaction::Disposition,
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
    },
    GetFullRecoveryState {
        store_id: store::Id,
        sender: std::sync::mpsc::Sender<FullStateResponse>
    }
}

struct FullStateResponse(Vec<TransactionRecoveryState>, Vec<AllocationRecoveryState>);