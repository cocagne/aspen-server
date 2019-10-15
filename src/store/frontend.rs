use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;

use super::backend::{Backend, Completion};
use super::ObjectCache;
use crate::network;
use crate::store;
use crate::transaction;
use crate::object;

struct PendingRead {
    client_id: network::ClientId,
    request_id: network::RequestId,
}

struct PRCache {
    vstack: Vec<Box<Vec<PendingRead>>>
}

impl PRCache {
    fn allocate(&mut self) -> Box<Vec<PendingRead>> {
        match self.vstack.pop() {
            Some(vpr) => vpr,
            None => Box::new(vec![])
        }
    }

    fn recycle(&mut self, mut vpr: Box<Vec<PendingRead>>) {
        vpr.clear();
        self.vstack.push(vpr)
    }
}


pub struct Frontend {
    store_id: store::Id,
    backend: Box<dyn Backend>,
    object_cache: Box<dyn ObjectCache>,
    pr_cache: PRCache,
    pending_reads: HashMap<object::Id, Box<Vec<PendingRead>>>
}

impl Frontend {
    pub fn new(
        store_id: store::Id,
        backend: Box<dyn Backend>,
        object_cache: Box<dyn ObjectCache>) -> Frontend {
        Frontend {
            store_id, 
            backend,
            object_cache,
            pr_cache: PRCache { vstack: Vec::new() },
            pending_reads: HashMap::new()
        }
    }

    pub fn id(&self) -> store::Id {
        self.store_id
    }

    pub fn backend_complete(&mut self, net: &Box<dyn network::Messenger>, completion: Completion) {
        match completion {
            Completion::Read{
                object_id,
                store_pointer,
                result,
                ..
            } => self.backend_complete_read(net, object_id, store_pointer, result),

            Completion::Commit {
                object_id,
                txid,
                result,
                ..
            } => self.backend_complete_commit(net, object_id, txid, result)
        }
    }

    fn backend_complete_read(
        &mut self,
        net: &Box<dyn network::Messenger>,
        object_id: object::Id,
        store_pointer: store::Pointer,
        result: Result<store::ReadState, store::ReadError>) {

        let read_result = result.clone();

        if let Ok(state) = result {

            let o = store::State {
                id: object_id,
                store_pointer,
                metadata: state.metadata,
                object_kind: state.object_kind,
                transaction_references: 0,
                locked_to_transaction: None,
                data: state.data.clone(),
                max_size: None,
                kv_state: None,
            };

            self.object_cache.insert(Rc::new(RefCell::new(o)));
        }

        if let Some(vpr) = self.pending_reads.remove(&object_id) {

            for pr in vpr.iter() {
                net.send_read_response(pr.client_id, pr.request_id, object_id, read_result.clone());
            }

            self.pr_cache.recycle(vpr);
        } 
    }

    fn backend_complete_commit(
        &mut self,
        _net: &Box<dyn network::Messenger>,
        _object_id: object::Id,
        _txid: transaction::Id,
        _result: Result<(), store::CommitError>) {
        
        unimplemented!()
    }

    pub fn read(
        &mut self, 
        net: &Box<dyn network::Messenger>,
        client_id: network::ClientId,
        request_id: network::RequestId,
        locater: &store::Locater) {
        
        if let Some(state) = self.object_cache.get(&locater.object_id) {
            let s = state.borrow();

            net.send_read_response(client_id, request_id, locater.object_id, Ok(store::ReadState {
                id: s.id,
                metadata: s.metadata,
                object_kind: s.object_kind,
                data: s.data.clone(),
            }));
            
        } else if let Some(vpr) = self.pending_reads.get_mut(&locater.object_id) {

            vpr.push(PendingRead{
                client_id,
                request_id
            });

        } else {

            let mut vpr = self.pr_cache.allocate();
            vpr.push(PendingRead{
                client_id,
                request_id
            });
            self.pending_reads.insert(locater.object_id, vpr);

            self.backend.read(locater);
        }
    }
}

