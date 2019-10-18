use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;

use either::{Either, Left, Right};

use super::backend::{Backend, Completion};
use super::ObjectCache;

use crate::crl;
use crate::network;
use crate::store;
use crate::transaction;
use crate::transaction::messages;
use crate::transaction::messages::Message;
use crate::transaction::tx::Tx;
use crate::object;

struct NetRead {
    client_id: network::ClientId,
    request_id: network::RequestId,
}

struct TxRead {
    transaction_id: transaction::Id
}

struct PendingReadCache {
    vstack: Vec<Box<Vec<Either<NetRead, TxRead>>>>
}

impl PendingReadCache {
    fn allocate(&mut self) -> Box<Vec<Either<NetRead, TxRead>>> {
        match self.vstack.pop() {
            Some(vpr) => vpr,
            None => Box::new(vec![])
        }
    }

    fn recycle(&mut self, mut vpr: Box<Vec<Either<NetRead, TxRead>>>) {
        vpr.clear();
        self.vstack.push(vpr)
    }
}

pub struct Frontend {
    store_id: store::Id,
    backend: Rc<dyn Backend>,
    object_cache: Box<dyn ObjectCache>,
    crl: Rc<dyn crl::Crl>,
    net: Rc<dyn network::Messenger>,
    pr_cache: PendingReadCache,
    accept_cache: messages::MessageCache<messages::Accept>,
    resolved_cache: messages::MessageCache<messages::Resolved>,
    pending_reads: HashMap<object::Id, Box<Vec<Either<NetRead, TxRead>>>>,
    transactions: HashMap<transaction::Id, transaction::tx::Tx>
}

impl Frontend {
    pub fn new(
        store_id: store::Id,
        backend: &Rc<dyn Backend>,
        object_cache: Box<dyn ObjectCache>,
        crl: &Rc<dyn crl::Crl>,
        net: &Rc<dyn network::Messenger>) -> Frontend {
        Frontend {
            store_id, 
            backend: backend.clone(),
            object_cache,
            crl: crl.clone(),
            net: net.clone(),
            pr_cache: PendingReadCache { vstack: Vec::new() },
            accept_cache: messages::MessageCache::new(50),
            resolved_cache: messages::MessageCache::new(50),
            pending_reads: HashMap::new(),
            transactions: HashMap::new()
        }
    }

    pub fn id(&self) -> store::Id {
        self.store_id
    }

    pub fn backend_complete(&mut self, completion: Completion) {
        match completion {
            Completion::Read{
                object_id,
                store_pointer,
                result,
                ..
            } => self.backend_read_completed(object_id, store_pointer, result),

            Completion::Commit {
                object_id,
                txid,
                result,
                ..
            } => self.backend_commit_completed(object_id, txid, result)
        }
    }

    fn backend_read_completed(
        &mut self,
        object_id: object::Id,
        store_pointer: store::Pointer,
        result: Result<store::ReadState, store::ReadError>) {

        let read_result = result.clone();

        match result {
            Ok(state) => {
                let o = Rc::new(RefCell::new(store::State {
                    id: object_id,
                    store_pointer,
                    metadata: state.metadata,
                    object_kind: state.object_kind,
                    transaction_references: 0,
                    locked_to_transaction: None,
                    data: state.data.clone(),
                    max_size: None,
                    kv_state: None,
                }));

                self.object_cache.insert(o.clone());

                if let Some(vpr) = self.pending_reads.remove(&object_id) {

                    for pr in vpr.iter() {
                        match pr {
                            Left(net_read) => {
                                self.net.send_read_response(net_read.client_id, 
                                    net_read.request_id, object_id, read_result.clone());
                            },
                            Right(tx_read) => {
                                if let Some(tx) = self.transactions.get_mut(&tx_read.transaction_id) {
                                    tx.object_loaded(&o);
                                }
                            }
                        }
                    }

                    self.pr_cache.recycle(vpr);
                }
            },
            Err(_) => {
        
                if let Some(vpr) = self.pending_reads.remove(&object_id) {

                    // TODO do something with the error message

                    self.pr_cache.recycle(vpr);
                }
            }
        }
    }

    fn backend_commit_completed(
        &mut self,
        object_id: object::Id,
        txid: transaction::Id,
        result: Result<(), store::CommitError>) {

        if let Some(tx) = self.transactions.get_mut(&txid) {
            tx.commit_complete(object_id, result);
        }
    }

    pub fn read_object_for_network(
        &mut self, 
        client_id: network::ClientId,
        request_id: network::RequestId,
        locater: &store::Locater) {
        
        if let Some(state) = self.object_cache.get(&locater.object_id) {
            let s = state.borrow();

            self.net.send_read_response(client_id, request_id, locater.object_id, Ok(store::ReadState {
                id: s.id,
                metadata: s.metadata,
                object_kind: s.object_kind,
                data: s.data.clone(),
            }));
            
        } else if let Some(vpr) = self.pending_reads.get_mut(&locater.object_id) {

            vpr.push(Left(NetRead{
                client_id,
                request_id
            }));

        } else {

            let mut vpr = self.pr_cache.allocate();
            vpr.push(Left(NetRead{
                client_id,
                request_id
            }));
            self.pending_reads.insert(locater.object_id, vpr);

            self.backend.read(locater);
        }
    }

    fn read_object_for_transaction(&mut self, transaction_id: transaction::Id, locater: &store::Locater) {
        if let Some(vpr) = self.pending_reads.get_mut(&locater.object_id) {
            vpr.push(Right(TxRead{transaction_id}));
        } else {
            let mut vpr = self.pr_cache.allocate();
            vpr.push(Right(TxRead{transaction_id}));
            self.pending_reads.insert(locater.object_id, vpr);
            self.backend.read(locater);
        }
    }

    pub fn crl_complete(&mut self, completion: crl::Completion) {
        match completion {
            crl::Completion::TransactionSave {
                transaction_id,
                save_id,
                success,
                ..
            } => {
                self.crl_tx_save_complete(transaction_id, save_id, success);
            },
            crl::Completion::AllocationSave {
                object_id,
                success,
                ..
            } => {
                self.crl_alloc_save_complete(object_id, success);
            }
        }
    }

    fn crl_tx_save_complete(
        &mut self,
        transaction_id: transaction::Id,
        save_id: crl::TxSaveId,
        success: bool) {
        
        if let Some(tx) = self.transactions.get_mut(&transaction_id) {
            tx.crl_tx_save_complete(save_id, success);
        }
    }

    fn crl_alloc_save_complete(&mut self, object_id: object::Id, success: bool) {
        
    }

    pub fn receive_transaction_message(&mut self, msg: Message) {
        match msg {
            Message::Prepare(m) => self.receive_prepare(m),
            Message::Accept(m) => {
                self.transactions.get_mut(&m.txid).map(|tx| tx.receive_accept(m));
            },
            Message::Resolved(m) => {
                self.transactions.get_mut(&m.txid).map(|tx| tx.receive_resolved(m));
            },
            Message::Finalized(m) => {
                self.transactions.get_mut(&m.txid).map(|tx| tx.receive_finalized(m));
            },
            Message::Heartbeat(m) => {
                self.transactions.get_mut(&m.txid).map(|tx| tx.receive_heartbeat(m));
            },
            Message::StatusRequest(m) => {
                self.transactions.get_mut(&m.txid).map(|tx| tx.receive_status_request(m));
            },
            _ => () // ignore
        }
    }

    pub fn receive_prepare(&mut self, msg: messages::Prepare) {

        let txid = msg.txd.id;

        match self.transactions.get_mut(&txid) {
            Some(tx) => tx.receive_prepare(msg),
            None => {

                let object_locaters = msg.txd.hosted_objects(self.store_id);

                let mut tx = Tx::new(self.store_id, msg, &object_locaters,
                    &self.backend, &self.crl, &self.net);

                // Start Loading objects

                let mut loaded: Vec<Rc<std::cell::RefCell<store::State>>> = Vec::new();

                for (object_id, pointer) in object_locaters {
                    match self.object_cache.get(&object_id) {
                        Some(obj) => {
                            loaded.push(obj.clone());
                        },
                        None => {
                            let locater = store::Locater { 
                                object_id: object_id,
                                pointer: pointer
                            };
                            self.read_object_for_transaction(txid, &locater);
                        }
                    }
                }
                
                // Prepare could be late. Check for Accept and Resolved messages
                if let Some(accept) = self.accept_cache.get(&txid) {
                    tx.receive_accept(accept);
                }
                if let Some(resolved) = self.resolved_cache.get(&txid) {
                    tx.receive_resolved(resolved);
                }

                for p in loaded {
                    tx.object_loaded(&p);
                }

                self.transactions.insert(txid, tx);
            }
        }
    }
}

