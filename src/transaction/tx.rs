use std::collections::{HashMap, HashSet};

use time;

use crate::data::{ArcData, ArcDataSlice};
use crate::network;
use crate::object;
use crate::paxos;
use crate::store;
use crate::transaction;

use super::messages::*;

enum LoadState {
    Loading {
        oprepare: Option<Prepare>,
        prep_responses: HashMap<store::Id, PrepareResponse>,
        
    },
    Ready {
        txd: transaction::TransactionDescription,
        serialized_txd: ArcData,
    }
}

struct PreTransactionOpportunisticRebuild {
    object_id: object::Id,
    required_metadata: object::Metadata,
    data: ArcDataSlice
}

pub struct Tx {
    txid: transaction::Id,
    acceptor: paxos::Acceptor,
    learner: paxos::Learner,
    load_state: LoadState,
    oresolution: Option<bool>,
    ofinalized: Option<bool>,
    object_updates: HashMap<object::Id, ArcDataSlice>,
    objects: HashMap<object::Id, store::TxStateRef>,
    pre_tx_rebuilds: Vec<PreTransactionOpportunisticRebuild>,
    last_event: time::PreciseTime,
    /// During Loading, these contain the ids that are still loading. During commit,
    /// it stores the Ids of objects being committed
    remaining_objects: HashSet<object::Id>,
}

impl Tx {
    pub fn new(store_id: store::Id, num_peers: u8, quorum_size: u8, txid: transaction::Id) -> Tx {
        Tx {
            txid,
            acceptor: paxos::Acceptor::new(store_id.pool_index, None),
            learner: paxos::Learner::new(num_peers, quorum_size),
            load_state: LoadState::Loading{
                oprepare: None,
                prep_responses: HashMap::new(),
            },
            oresolution: None,
            ofinalized: None,
            object_updates: HashMap::new(),
            objects: HashMap::new(),
            pre_tx_rebuilds: Vec::new(),
            last_event: time::PreciseTime::now(),
            remaining_objects: HashSet::new()
        }
    }

    pub fn resolved(&self) -> Option<bool> {
        return self.oresolution;
    }   

    pub fn finalized(&self) -> Option<bool> {
        return self.ofinalized;
    }

    pub fn receive_prepare(&mut self, m: Prepare, net: &Box<dyn network::Messenger>) {

    }

    pub fn receive_finalized(&mut self, value: bool) {
        self.ofinalized = Some(value);
        self.on_resolution(value)
    }

    pub fn receive_resolved(&mut self, value: bool) {
        self.on_resolution(value)
    }

    fn on_resolution(&mut self, value:bool) {
        if self.oresolution.is_none() {
            self.oresolution = Some(value);
        } 
    }

}