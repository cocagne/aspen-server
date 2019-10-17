use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;

use either::{Either, Left, Right};

use super::backend::{Backend, Completion};
use super::ObjectCache;
use crate::network;
use crate::store;
use crate::transaction;
use crate::transaction::messages;
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
    backend: Box<dyn Backend>,
    object_cache: Box<dyn ObjectCache>,
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
        backend: Box<dyn Backend>,
        object_cache: Box<dyn ObjectCache>,
        net: &Rc<dyn network::Messenger>) -> Frontend {
        Frontend {
            store_id, 
            backend,
            object_cache,
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
            } => self.backend_complete_read(object_id, store_pointer, result),

            Completion::Commit {
                object_id,
                txid,
                result,
                ..
            } => self.backend_complete_commit(object_id, txid, result)
        }
    }

    fn backend_complete_read(
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
                                    tx.objects.insert(object_id, store::TxStateRef::new(&o));
                                    tx.pending_object_loads -= 1;
                                    if tx.pending_object_loads == 0 {
                                        tx.objects_loaded();
                                    }
                                }
                            }
                        }
                        
                    }

                    self.pr_cache.recycle(vpr);
                }
            },
            Err(err) => {
        
                if let Some(vpr) = self.pending_reads.remove(&object_id) {

                    // TODO do something with the error message

                    self.pr_cache.recycle(vpr);
                }
            }
        }
    }

    fn backend_complete_commit(
        &mut self,
        _object_id: object::Id,
        _txid: transaction::Id,
        _result: Result<(), store::CommitError>) {
        
        unimplemented!()
    }

    pub fn read(
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

    pub fn receive_prepare(
        &mut self,
        msg: messages::Prepare) {

        let txid = msg.txd.id;

        match self.transactions.get_mut(&txid) {
            Some(tx) => tx.receive_prepare(msg, &self.net),
            None => {
                let objects_to_load = msg.txd.hosted_objects(self.store_id);
                
                let mut tx = Tx::new(self.store_id, msg);

                // Prepare could be late. Check for Accept and Resolved messages
                if let Some(accept) = self.accept_cache.get(&txid) {
                    tx.receive_accept(accept, &self.net);
                }
                if let Some(resolved) = self.resolved_cache.get(&txid) {
                    tx.receive_resolved(resolved.value);
                }

                // Begin loading objects

                tx.pending_object_loads = objects_to_load.len();

                for (object_id, pointer) in objects_to_load {
                    match self.object_cache.get(&object_id) {
                        Some(obj) => {
                            tx.objects.insert(object_id, store::TxStateRef::new(obj));
                            tx.pending_object_loads -= 1;
                        },
                        None => {
                            let locater = store::Locater { 
                                object_id, 
                                pointer
                            };
                            self.read_object_for_transaction(txid, &locater);
                        }
                    }
                }

                if tx.pending_object_loads == 0 {
                    tx.objects_loaded();
                }

                self.transactions.insert(txid, tx);
            }
        }
    }
}

