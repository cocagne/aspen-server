
use std::sync;

use crate::object;
use crate::store;

use super::{Pointer, AllocationError, Locater, State};

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, Hash)]
pub struct PutId(pub u64);

pub trait CompletionHandler {
    fn complete(&self, op: Completion);
}

#[derive(Debug)]
pub enum Completion {
    Get {
        store_id: store::Id,
        object_id: object::Id,
        result: Result<store::ReadState, store::ReadError>
    },
    Put {
        store_id: store::Id,
        object_id: object::Id,
        put_id: PutId,
        result: Result<(), store::PutError>
    }
}

impl Completion {
    pub fn store_id(&self) -> store::Id {
        match self {
            Completion::Get{store_id, ..} => *store_id,
            Completion::Put{store_id, ..} => *store_id,
        }
    }
}

pub trait Backend {

    fn set_completion_handler(&mut self, handler: Box<dyn CompletionHandler>);

    fn allocate(
        &mut self,
        id: object::Id,
        object_kind: object::Kind,
        metadata: object::Metadata,
        data: sync::Arc<Vec<u8>>,
        max_size: Option<u32>
    ) -> Result<Pointer, AllocationError>;

    fn get(&mut self, locater: Locater);

    fn put(&mut self, state: State) -> PutId;
}