use std::collections::HashMap;

use super::backend::{Backend, Completion, PutId};
use super::messages::{In, Out};
use super::ObjectCache;
use crate::network;
use crate::store;
use crate::object;

pub enum Response {
    Single(Out),
    Multiple(Vec<Out>)
}

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
    pub fn receive(&mut self, message: In) -> Option<Response> {
        match message {
            In::Read {
                client_id,
                request_id,
                store_id,
                locater
            } => self.read(client_id, request_id, store_id, locater),
        } 
    }

    pub fn complete(&mut self, completion: Completion) -> Option<Response> {
        match completion {
            Completion::Get{
                object_id,
                result,
                ..
            } => self.complete_get(object_id, result),

            Completion::Put {
                object_id,
                put_id,
                result,
                ..
            } => self.complete_put(object_id, put_id, result)
        }
    }

    fn complete_get(
        &mut self,
        object_id: object::Id,
        result: Result<store::ReadState, store::ReadError>) -> Option<Response> {

        let store_id = self.store_id.clone();

        let out = |pr: &PendingRead| -> Out {
            Out::Read {
                client_id: pr.client_id.clone(),
                request_id: pr.request_id.clone(),
                store_id: store_id.clone(),
                result: result.clone()
            }
        };
        
        if let Some(vpr) = self.pending_reads.remove(&object_id) {

            let response = if vpr.len() == 1 {
                Response::Single(out(&vpr[0]))
            } else {
                Response::Multiple(vpr.iter().map(out).collect())
            };

            self.pr_cache.recycle(vpr);

            Some(response)
        } else {
            None
        }
    }

    fn complete_put(
        &mut self,
        _object_id: object::Id,
        _put_id: PutId,
        _result: Result<(), store::PutError>) -> Option<Response> {
        
        unimplemented!()
    }

    fn read(
        &mut self, 
        client_id: network::ClientId,
        request_id: network::RequestId,
        store_id: store::Id,
        locater: store::Locater) -> Option<Response> {
        
        if let Some(state) = self.object_cache.get(&locater.object_id) {
            let s = state.borrow();

            let out = Out::Read {
                client_id,
                request_id,
                store_id,
                result: Ok(store::ReadState {
                    id: s.id,
                    store_pointer: s.store_pointer.clone(),
                    metadata: s.metadata,
                    object_kind: s.object_kind,
                    data: s.data.clone(),
                    crc: s.crc
                })
            };

            Some(Response::Single(out))

        } else if let Some(vpr) = self.pending_reads.get_mut(&locater.object_id) {

            vpr.push(PendingRead{
                client_id,
                request_id
            });

            None

        } else {

            let mut vpr = self.pr_cache.allocate();
            vpr.push(PendingRead{
                client_id,
                request_id
            });
            self.pending_reads.insert(locater.object_id, vpr);

            self.backend.get(locater);

            None
        }
    }
}

