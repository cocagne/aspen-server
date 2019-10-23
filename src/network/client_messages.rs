
use crate::data::ArcDataSlice;
use crate::hlc;
use crate::network;
use crate::object;
use crate::store;
use crate::transaction;

pub struct Allocate {
    pub to: store::Id,
    pub from: network::ClientId,
    pub new_object_id: object::Id,
    pub kind: object::Kind,
    pub max_size: Option<u32>,
    pub initial_refcount: object::Refcount,
    pub data: ArcDataSlice,
    pub timestamp: hlc::Timestamp,
    pub allocation_transaction_id: transaction::Id,
    pub revision_guard: object::AllocationRevisionGuard
}

#[derive(Debug, Clone)]
pub struct AllocateResponse {
    pub to: network::ClientId,
    pub from: store::Id,
    pub allocation_transaction_id: transaction::Id,
    pub object_id: object::Id,
    pub result: Result<store::Pointer, store::AllocationError>
}

pub enum Message {
    Allocate(Allocate),
    AllocateResponse(AllocateResponse)
}
