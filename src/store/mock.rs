use std::collections::HashMap;

use crc32fast::Hasher;

use crate::object;
use super::backend;
use super::backend::{Completion, PutId};
use crate::store;
use super::*;

struct Obj {
    id: object::Id,
    store_pointer: Pointer,
    metadata: object::Metadata,
    object_kind: object::Kind,
    data: sync::Arc<Vec<u8>>,
    crc: Crc32
}

pub struct MockStore {
    store_id: store::Id,
    completion_handler: Box<dyn backend::CompletionHandler>,
    content: HashMap<object::Id, Obj>,
    put_id: u64
}

impl backend::Backend for MockStore {
    fn set_completion_handler(&mut self, handler: Box<dyn backend::CompletionHandler>) {
        self.completion_handler = handler;
    }

    fn allocate(
        &mut self,
        id: object::Id,
        object_kind: object::Kind,
        metadata: object::Metadata,
        data: sync::Arc<Vec<u8>>,
        _max_size: Option<u32>
    ) -> Result<Pointer, AllocationError> {
        let mut h = Hasher::new();
        h.update(&*data);
        let crc = Crc32(h.finalize());
        self.content.insert(id, Obj {
            id,
            store_pointer: Pointer::None{pool_index: 0},
            metadata,
            object_kind,
            data,
            crc
        });
        Ok(Pointer::None{pool_index: 0})
    }

    fn get(&mut self, locater: Locater) {
        let result = match self.content.get(&locater.object_id) {
            None => Err(ReadError::NotFound),
            Some(s) => Ok(ReadState {
                id: s.id,
                metadata: s.metadata,
                object_kind: s.object_kind,
                data: s.data.clone(),
            })
        };
        let _ = self.completion_handler.complete(Completion::Get{
            store_id: self.store_id,
            object_id: locater.object_id,
            result
        });
    }

    fn put(&mut self, state: State) -> PutId {
        let put_id = self.put_id;
        self.put_id += 1;

        self.content.insert(state.id, Obj {
            id : state.id,
            store_pointer: state.store_pointer,
            metadata: state.metadata,
            object_kind: state.object_kind,
            data: state.data.clone(),
            crc: state.crc
        });

        let _ = self.completion_handler.complete(Completion::Put{
            store_id: self.store_id,
            object_id: state.id,
            put_id: PutId(put_id),
            result: Ok(())
        });

        PutId(put_id)
    }
}