use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;

use crate::data::ArcDataSlice;
use crate::object;
use super::backend;
use super::backend::Completion;
use crate::store;
use crate::transaction;
use super::*;


struct Obj {
    id: object::Id,
    metadata: object::Metadata,
    object_kind: object::Kind,
    data: sync::Arc<Vec<u8>>,
}

pub struct MockStore {
    store_id: store::Id,
    completion_handler: Box<dyn backend::CompletionHandler>,
    content: RefCell<HashMap<object::Id, Obj>>,
}

struct NullHandler;

impl backend::CompletionHandler for NullHandler {
    fn complete(&self, _: Completion) {}
}

impl MockStore {
    pub fn new(store_id: store::Id) -> Rc<dyn backend::Backend> {
        Rc::new(MockStore{
            store_id,
            completion_handler: Box::new(NullHandler),
            content: RefCell::new(HashMap::new())
        })
    }
}

impl backend::Backend for MockStore {

    fn set_completion_handler(&mut self, handler: Box<dyn backend::CompletionHandler>) {
        self.completion_handler = handler;
    }

    fn allocate(
        &self,
        id: object::Id,
        object_kind: object::Kind,
        metadata: object::Metadata,
        data: ArcDataSlice,
        _max_size: Option<u32>
    ) -> Result<Pointer, AllocationError> {

        let mut content = self.content.borrow_mut();

        content.insert(id, Obj {
            id,
            metadata,
            object_kind,
            data: sync::Arc::new(data.to_vec()),
        });
        Ok(Pointer::None{pool_index: 0})
    }

    fn abort_allocation(&self, object_id: object::Id) {
        let mut content = self.content.borrow_mut();
        content.remove(&object_id);
    }

    fn read(&self, locater: &Locater) {

        let content = self.content.borrow_mut();

        let result = match content.get(&locater.object_id) {
            None => Err(ReadError::ObjectNotFound),
            Some(s) => Ok(ReadState {
                id: s.id,
                metadata: s.metadata,
                object_kind: s.object_kind,
                data: s.data.clone(),
            })
        };
        let _ = self.completion_handler.complete(Completion::Read{
            store_id: self.store_id,
            object_id: locater.object_id,
            store_pointer: locater.pointer.clone(),
            result
        });
    }

    fn commit(&self, state: backend::CommitState, txid: transaction::Id) {
        
        let mut content = self.content.borrow_mut();

        content.insert(state.id, Obj {
            id : state.id,
            metadata: state.metadata,
            object_kind: state.object_kind,
            data: state.data.clone(),
        });

        let _ = self.completion_handler.complete(Completion::Commit{
            store_id: self.store_id,
            object_id: state.id,
            txid,
            result: Ok(())
        });
    }
}