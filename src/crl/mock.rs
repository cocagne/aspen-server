use std::thread;
use std::sync;

use crossbeam::crossbeam_channel;

use crate::crl::{RequestCompletionHandler, Crl, Completion};
use crate::store;

use super::*;

#[derive(Clone, Copy)]
struct ClientId(usize);

struct RegisterClientResponse {
    client_id: ClientId
}

enum Request {
    Tx(ClientId, store::Id, transaction::Id, TxSaveId),
    Alloc(ClientId, store::Id, transaction::Id, object::Id),
    RegisterClientRequest {
        sender: crossbeam_channel::Sender<RegisterClientResponse>,
        handler: sync::Arc<dyn RequestCompletionHandler + Send + Sync>
    },
    Terminate
}

struct Backend {
    io_threads: Vec<thread::JoinHandle<()>>,
    sender: crossbeam_channel::Sender<Request>
}

pub fn create() -> Box<dyn crate::crl::Backend> {
    let (sender, receiver) = crossbeam_channel::unbounded();
    let thr = thread::spawn(|| io_thread(receiver));
    Box::new(Backend {
        io_threads: vec![thr],
        sender
    })
}

fn io_thread(receiver: crossbeam_channel::Receiver<Request>) {
    let mut clients: Vec<sync::Arc<dyn RequestCompletionHandler + Send + Sync>> = Vec::new();

    loop {
        match receiver.recv().unwrap() {
            Request::Tx(client, store_id, transaction_id, save_id) => {
                clients[client.0].complete(
                    Completion::TransactionSave { store_id, transaction_id, save_id, success:true })
            },
            Request::Alloc(client, store_id, transaction_id, object_id) => {
                clients[client.0].complete(
                    Completion::AllocationSave { store_id, transaction_id, object_id, success:true })
            },
            Request::RegisterClientRequest {
                sender,
                handler
            } => {
                let client_id = clients.len();
                clients.push(handler);
                let _ = sender.send(RegisterClientResponse{ client_id: ClientId(client_id) });
            },
            Request::Terminate => break
        }
    }
}

impl crate::crl::Backend for Backend {
    fn shutdown(&mut self) {
        let _ = self.sender.send(Request::Terminate);
        while !self.io_threads.is_empty() {
            self.io_threads.pop().map(|t| t.join());
        }
    }

    fn new_interface(&self, 
        save_handler: sync::Arc<dyn RequestCompletionHandler + Send + Sync>) -> Box<dyn Crl> {

        let (response_sender, receiver) = crossbeam_channel::unbounded();

        self.sender.send(Request::RegisterClientRequest{sender: response_sender, handler: save_handler}).unwrap();

        let client_id = receiver.recv().unwrap().client_id;

        Box::new(Frontend::new(client_id, self.sender.clone()))
    }
}

struct Frontend {
    client_id: ClientId,
    sender: crossbeam_channel::Sender<Request>,
}

impl Frontend {

    pub(super) fn new(client_id: ClientId, sender: crossbeam_channel::Sender<Request>) -> Frontend {
        Frontend { 
            client_id: client_id,
            sender: sender,
        }
    }
}

impl crate::crl::Crl for Frontend {

    fn get_full_recovery_state(&self, _store_id: store::Id) -> 
        (Vec<TransactionRecoveryState>, Vec<AllocationRecoveryState>) {
        (Vec::new(), Vec::new())
    }

    fn save_transaction_state(
        &self,
        store_id: store::Id,
        transaction_id: transaction::Id,
        _serialized_transaction_description: ArcDataSlice,
        _object_updates: Option<Vec<transaction::ObjectUpdate>>,
        _tx_disposition: transaction::Disposition,
        _paxos_state: paxos::PersistentState,
        save_id: TxSaveId
    ) {
        // Explicitly ignore any errors
        let _ = self.sender.send(Request::Tx(self.client_id, store_id, transaction_id, save_id)); 
    }

    fn drop_transaction_object_data(
        &self,
        _store_id: store::Id,
        _transaction_id: transaction::Id
    ) {}

    fn delete_transaction_state(
        &self,
        _store_id: store::Id,
        _transaction_id: transaction::Id
    ) {}

    fn save_allocation_state(
        &self,
        store_id: store::Id,
        _store_pointer: store::Pointer,
        id: object::Id,
        _kind: object::Kind,
        _size: Option<u32>,
        _data: ArcDataSlice,
        _refcount: object::Refcount,
        _timestamp: hlc::Timestamp,
        allocation_transaction_id: transaction::Id,
        _serialized_revision_guard: ArcDataSlice
    ) {
        // Explicitly ignore any errors
        let _ = self.sender.send(Request::Alloc(self.client_id, store_id, allocation_transaction_id, id));
    }

    fn delete_allocation_state(
        &self,
        _store_id: store::Id, 
        _allocation_transaction_id: transaction::Id) {}
}