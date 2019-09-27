
use std::sync;

use crossbeam::crossbeam_channel;

use crate::object;
use crate::store;

use super::{Pointer, AllocationError, Locater, State};

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, Hash)]
pub struct PutId(pub u64);

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

pub trait Backend {

    fn set_completion_sender(&mut self, sender: crossbeam_channel::Sender<Completion>);

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