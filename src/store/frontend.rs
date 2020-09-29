use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;

use either::{Either, Left, Right};

use super::backend;
use super::backend::{Backend, Completion};
use super::ObjectCache;

use crate::crl;
use crate::data::ArcDataSlice;
use crate::network;
use crate::network::client_messages;
use crate::store;
use crate::transaction;
use crate::transaction::messages;
use crate::transaction::messages::Message;
use crate::transaction::tx::Tx;
use crate::object;
use crate::paxos;

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

enum PendingAllocation {
    Single(client_messages::AllocateResponse),
    Multi(Vec<client_messages::AllocateResponse>)
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
    pending_allocations: HashMap<transaction::Id, PendingAllocation>,
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
            pending_allocations: HashMap::new(),
            transactions: HashMap::new()
        }
    }

    pub fn id(&self) -> store::Id {
        self.store_id
    }

    pub fn clear_cache(&mut self) {
        self.object_cache.clear();
    }

    pub fn load_recovery_state(&mut self,
        tx_recovery: Vec<crl::TransactionRecoveryState>, 
        alloc_recovery: Vec<crl::AllocationRecoveryState>) {

        for txr in tx_recovery {
            let proposal_id = match (txr.paxos_state.promised, txr.paxos_state.accepted) {
                (None, None) => paxos::ProposalId::initial_proposal_id(0),
                (Some(pid), None) => pid,
                (None, Some((pid, _))) => pid,
                (Some(_), Some((pid, _))) => pid
            };
            let mut object_updates: HashMap<object::Id, ArcDataSlice> = HashMap::new();
            for ou in txr.object_updates {
                object_updates.insert(ou.object_id, ou.data);
            }
            let p = messages::Prepare { 
                to: txr.store_id,
                from: txr.store_id,
                proposal_id : proposal_id,
                txd: transaction::TransactionDescription::deserialize(&txr.serialized_transaction_description),
                object_updates: object_updates,
                pre_tx_rebuilds: Vec::new()
            };
            let object_locaters: HashMap<object::Id, store::Pointer> = p.txd.hosted_objects(txr.store_id);

            let mut t = Tx::new(txr.store_id, p, &object_locaters, &self.backend, &self.crl, &self.net);
            t.load_saved_state(txr.tx_disposition, txr.paxos_state);
            self.transactions.insert(txr.transaction_id, t); 
        }

        for ar in alloc_recovery {
            let ar = client_messages::AllocateResponse {
                to: network::ClientId::null(),
                from: self.store_id,
                allocation_transaction_id: ar.allocation_transaction_id,
                object_id: ar.id,
                result: Ok(ar.store_pointer)
            };
            match self.pending_allocations.get_mut(&ar.allocation_transaction_id) {
                None => {
                    self.pending_allocations.insert(ar.allocation_transaction_id, PendingAllocation::Single(ar));
                },
                Some(pa) => {
                    match pa {
                        PendingAllocation::Single(p) => {
                            let alloc_tx_id = ar.allocation_transaction_id;
                            let v = vec![p.clone(), ar];
                            self.pending_allocations.insert(alloc_tx_id, PendingAllocation::Multi(v));
                        },
                        PendingAllocation::Multi(v) => v.push(ar)
                    }
                }
            }
        }
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
            Err(e) => {
        
                if let Some(vpr) = self.pending_reads.remove(&object_id) {

                    for pr in vpr.iter() {
                        match pr {
                            Left(net_read) => {
                                self.net.send_read_response(net_read.client_id, 
                                    net_read.request_id, object_id, Err(e));
                            },
                            Right(tx_read) => {
                                if let Some(tx) = self.transactions.get_mut(&tx_read.transaction_id) {
                                    tx.object_load_failed(&object_id);
                                }
                            }
                        }
                    }

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
                transaction_id,
                object_id,
                success,
                ..
            } => {
                self.crl_alloc_save_complete(transaction_id, object_id, success);
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

    pub fn allocate_object(&mut self, msg: client_messages::Allocate) {
        let metadata = object::Metadata {
            revision: object::Revision(msg.allocation_transaction_id.0),
            refcount: msg.initial_refcount,
            timestamp: msg.timestamp
        };

        let r = self.backend.allocate(msg.new_object_id, msg.kind, metadata, 
            msg.data.clone(), msg.max_size);

        match r {
            Err(e) => {
                let m = client_messages::AllocateResponse {
                    to: msg.from,
                    from: msg.to,
                    allocation_transaction_id: msg.allocation_transaction_id,
                    object_id: msg.new_object_id,
                    result: Err(e)
                };
                self.net.send_client_message(client_messages::Message::AllocateResponse(m));
            },
            Ok(pointer) => {

                let data = std::sync::Arc::new(msg.data.to_vec());

                let s = store::State {
                    id: msg.new_object_id,
                    store_pointer: pointer.clone(),
                    metadata,
                    object_kind: msg.kind,
                    transaction_references: 1, // Ensure this stays in cache till alloc resolves
                    locked_to_transaction: None,
                    data: data.clone(),
                    max_size: msg.max_size,
                    kv_state: None
                };

                self.object_cache.insert(std::rc::Rc::new(std::cell::RefCell::new(s)));

                let m = client_messages::AllocateResponse {
                    to: msg.from,
                    from: msg.to,
                    allocation_transaction_id: msg.allocation_transaction_id,
                    object_id: msg.new_object_id,
                    result: Ok(pointer.clone())
                };

                if let Some(pa) = self.pending_allocations.remove(&msg.allocation_transaction_id) {
                    let mut v = match pa {
                        PendingAllocation::Single(m1) => {
                            let mut v = Vec::with_capacity(2);
                            v.push(m1);
                            v
                        },
                        PendingAllocation::Multi(v) => v
                    };
                    v.push(m);
                    self.pending_allocations.insert(msg.allocation_transaction_id, 
                        PendingAllocation::Multi(v));
                } else {
                    self.pending_allocations.insert(msg.allocation_transaction_id, 
                        PendingAllocation::Single(m));
                }

                self.crl.save_allocation_state(
                    self.store_id,
                    pointer.clone(),
                    msg.new_object_id,
                    msg.kind,
                    msg.max_size,
                    ArcDataSlice::from_bytes(&data[..]),
                    msg.initial_refcount,
                    msg.timestamp,
                    msg.allocation_transaction_id,
                    ArcDataSlice::from_vec(Vec::new()) // FIXME Replace with serialized guard
                );
            }
        }   
    }

    fn crl_alloc_save_complete(
        &mut self,
        transaction_id: transaction::Id,
        object_id: object::Id,
        _success: bool) {

        self.pending_allocations.get(&transaction_id).map(|pa| {
            match pa {
                PendingAllocation::Single(msg) => {
                    self.net.send_client_message(client_messages::Message::AllocateResponse(msg.clone()));
                },
                PendingAllocation::Multi(v) => {
                    for msg in v {
                        if msg.object_id == object_id {
                            self.net.send_client_message(client_messages::Message::AllocateResponse(msg.clone()));
                            break;
                        }
                    }
                }
            } 
        });
    }

    pub fn receive_client_message(&mut self, msg: client_messages::Message) {
        match msg {
            client_messages::Message::Allocate(m) => self.allocate_object(m),
            _ => () 
        }
    }

    pub fn receive_transaction_message(&mut self, msg: Message) {
        match msg {
            Message::Prepare(m) => self.receive_prepare(m),
            Message::Accept(m) => {
                self.transactions.get_mut(&m.txid).map(|tx| tx.receive_accept(m));
            },
            Message::Resolved(m) => {
                self.receive_resolved(&m);
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

    pub fn receive_resolved(&mut self, msg: &messages::Resolved) {
        // Check to see if any allocations are part of this transaction
        if let Some(pa) = self.pending_allocations.remove(&msg.txid) {
            if msg.value {
                let mut add = |m: client_messages::AllocateResponse| {
                    let o = self.object_cache.get(&m.object_id).unwrap();
                    let mut o = o.borrow_mut();

                    o.transaction_references -= 1;

                    let cs = backend::CommitState {
                        id: m.object_id,
                        store_pointer: o.store_pointer.clone(),
                        metadata: o.metadata,
                        object_kind: o.object_kind,
                        data: o.data.clone()
                    };
                    
                    self.backend.commit(cs, m.allocation_transaction_id);
                };

                match pa {
                    PendingAllocation::Single(m) => add(m),
                    PendingAllocation::Multi(v) => {
                        for m in v {
                            add(m);
                        }
                    }
                }
            } else {
                let mut rem = |m: client_messages::AllocateResponse| {
                    self.object_cache.remove(&m.object_id);
                    self.backend.abort_allocation(m.object_id);
                };

                match pa {
                    PendingAllocation::Single(m) => rem(m),
                    PendingAllocation::Multi(v) => {
                        for m in v {
                            rem(m);
                        }
                    }
                }
            }
        }
    }

    pub fn receive_prepare(&mut self, msg: messages::Prepare) {

        let txid = msg.txd.id;

        match self.transactions.get_mut(&txid) {
            Some(tx) => tx.receive_prepare(msg.from, msg.proposal_id),
            None => {

                let from = msg.from;
                let proposal_id = msg.proposal_id;

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

                tx.receive_prepare(from, proposal_id);

                self.transactions.insert(txid, tx);
            }
        }
    }
}

