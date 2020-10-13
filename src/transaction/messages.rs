
use std::collections::{HashMap, BTreeMap};

use time;
use uuid;

use crate::data::ArcDataSlice;
use crate::object;
use crate::paxos;
use crate::store;
use crate::transaction;

#[derive(Debug, Copy, Clone)]
pub enum PrepareResult {
    Nack(paxos::ProposalId),
    Promise(Option<(paxos::ProposalId, bool)>)
}

#[derive(Debug, Copy, Clone)]
pub enum AcceptResult {
    Nack(paxos::ProposalId),
    Accepted(bool)
}

pub struct PreTransactionOpportunisticRebuild {
    pub object_id: object::Id,
    pub required_metadata: object::Metadata,
    pub data: ArcDataSlice
}   

pub struct Prepare {
    pub to: store::Id,
    pub from: store::Id,
    pub proposal_id: paxos::ProposalId,
    pub txd: transaction::TransactionDescription,
    pub object_updates: HashMap<object::Id, ArcDataSlice>,
    pub pre_tx_rebuilds: Vec<PreTransactionOpportunisticRebuild>,
}

#[derive(Clone)]
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

impl Message {
    pub fn to_store(&self) -> store::Id {
        match self {
            Message::Prepare(m) => m.to,
            Message::PrepareResponse(m) => m.to,
            Message::Accept(m) => m.to,
            Message::AcceptResponse(m) => m.to,
            Message::Resolved(m) => m.to,
            Message::Committed(m) => m.to,
            Message::Finalized(m) => m.to,
            Message::Heartbeat(m) => m.to,
            Message::StatusRequest(m) => m.to,
            Message::StatusResponse(m) => m.to,
        }
    }

    pub fn get_txid(&self) -> transaction::Id {
        match self {
            Message::Prepare(m) => m.txd.id,
            Message::PrepareResponse(m) => m.txid,
            Message::Accept(m) => m.txid,
            Message::AcceptResponse(m) => m.txid,
            Message::Resolved(m) => m.txid,
            Message::Committed(m) => m.txid,
            Message::Finalized(m) => m.txid,
            Message::Heartbeat(m) => m.txid,
            Message::StatusRequest(m) => m.txid,
            Message::StatusResponse(m) => m.txid,
        }
    }
}

pub struct MessageCache<T> {
    btmap: BTreeMap<time::SteadyTime, transaction::Id>,
    hmap: HashMap<transaction::Id, (T, time::SteadyTime)>,
    max_entries: usize,
    num_entries: usize
}

impl<T> MessageCache<T> {
    pub fn new(max_entries: usize) -> MessageCache<T> {
        MessageCache {
            btmap: BTreeMap::new(),
            hmap: HashMap::new(),
            max_entries,
            num_entries: 0
        }
    }

    pub fn cache(&mut self, txid: transaction::Id, msg: T) {
        let now = time::SteadyTime::now();
        self.hmap.insert(txid, (msg, now.clone()));
        self.btmap.insert(now.clone(), txid);

        if self.num_entries == self.max_entries {
            let (ts, txid) = self.btmap.iter().next().unwrap();
            let ts = ts.clone();
            let txid = txid.clone();
            self.btmap.remove(&ts);
            self.hmap.remove(&txid);
            self.num_entries -= 1;
        }

        self.num_entries += 1;
    }

    pub fn get(&mut self, txid: &transaction::Id) -> Option<T> {
        match self.hmap.remove(txid) {
            None => None,
            Some((msg, ts)) => {
                self.btmap.remove(&ts);
                Some(msg)
            }
        }
    }
}