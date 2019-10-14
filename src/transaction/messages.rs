
use uuid;

use crate::data::ArcData;
use crate::object;
use crate::paxos;
use crate::store;

use crate::transaction;

pub enum PrepareResult {
    Nack(paxos::ProposalId),
    Promise(Option<(paxos::ProposalId, bool)>)
}

pub enum AcceptResult {
    Nack(paxos::ProposalId),
    Accepted(bool)
}

pub struct Prepare {
    pub to: store::Id,
    pub from: store::Id,
    pub proposal_id: paxos::ProposalId,
    pub txd: transaction::TransactionDescription,
    pub serialized_txd: ArcData
}
pub struct PrepareResponse {
    pub to: store::Id,
    pub from: store::Id,
    pub txid: transaction::Id,
    pub proposal_id: paxos::ProposalId,
    pub response: PrepareResult,
    pub disposition: transaction::Disposition
}
pub struct Accept {
    pub to: store::Id,
    pub from: store::Id,
    pub txid: transaction::Id,
    pub proposal_id: paxos::ProposalId,
    pub value: bool
}
pub struct AcceptResponse {
    pub to: store::Id,
    pub from: store::Id,
    pub txid: transaction::Id,
    pub proposal_id: paxos::ProposalId,
    pub response: AcceptResult
}
pub struct Resolved {
    pub to: store::Id,
    pub from: store::Id,
    pub txid: transaction::Id,
    pub value: bool
}
pub struct Committed {
    pub to: store::Id,
    pub from: store::Id,
    pub txid: transaction::Id,
    /// List of object UUIDs that could not be committed due to transaction requirement errors
    pub object_commit_errors: Vec<object::Id>
}
pub struct Finalized {
    pub to: store::Id,
    pub from: store::Id,
    pub txid: transaction::Id,
    pub value: bool
}
pub struct Heartbeat {
    pub to: store::Id,
    pub from: store::Id,
    pub txid: transaction::Id,
}
pub struct StatusRequest {
    pub to: store::Id,
    pub from: store::Id,
    pub txid: transaction::Id,
    pub request_uuid: uuid::Uuid
}
pub struct StatusResponse {
    pub to: store::Id,
    pub from: store::Id,
    pub txid: transaction::Id,
    pub request_uuid: uuid::Uuid,
    pub status: transaction::Status,
    pub finalized: bool
}

pub enum Message {
    Prepare(Prepare),
    PrepareResponse(PrepareResponse),
    Accept(Accept),
    AcceptResponse(AcceptResponse),
    Resolved(Resolved),
    Committed(Committed),
    Finalized(Finalized),
    Heartbeat(Heartbeat),
    StatusRequest(StatusRequest),
    StatusResponse(StatusResponse)
}
