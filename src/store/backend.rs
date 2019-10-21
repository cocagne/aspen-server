
use std::sync;

use crate::object;
use crate::store;
use crate::transaction;

use super::{Pointer, AllocationError, Locater, State};

pub trait CompletionHandler {
    fn complete(&self, op: Completion);
}

#[derive(Debug)]
pub enum Completion {
    Read {
        store_id: store::Id,
        object_id: object::Id,
        store_pointer: Pointer,
        result: Result<store::ReadState, store::ReadError>
    },
    Commit {
        store_id: store::Id,
        object_id: object::Id,
        txid: transaction::Id,
        result: Result<(), store::CommitError>
    }
}

impl Completion {
    pub fn store_id(&self) -> store::Id {
        match self {
            Completion::Read{store_id, ..} => *store_id,
            Completion::Commit{store_id, ..} => *store_id,
        }
    }
}

pub struct CommitState {
    pub id: object::Id,
    pub store_pointer: Pointer,
    pub metadata: object::Metadata,
    pub object_kind: object::Kind,
    pub data: sync::Arc<Vec<u8>>,
}

pub trait Backend {

    fn set_completion_handler(&mut self, handler: Box<dyn CompletionHandler>);

    fn allocate(
        &self,
        id: object::Id,
        object_kind: object::Kind,
        metadata: object::Metadata,
        data: sync::Arc<Vec<u8>>,
        max_size: Option<u32>
    ) -> Result<Pointer, AllocationError>;

    fn read(&self, locater: &Locater);

    fn commit(&self, state: CommitState, txid: transaction::Id);
}