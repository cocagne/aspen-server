
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

    fn read(&mut self, locater: &Locater);

    fn commit(&mut self, state: State, txid: transaction::Id);
}