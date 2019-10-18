use std::collections::HashMap;
use std::rc::Rc;

use time;

use crate::crl;
use crate::data::ArcDataSlice;
use crate::network;
use crate::object;
use crate::paxos;
use crate::store;
use crate::store::backend::Backend;
use crate::transaction;

use super::messages::*;

struct LastPrepare {
    from: store::Id,
    proposal_id: paxos::ProposalId,
    save_id: crl::TxSaveId
}

pub struct Tx {
    txid: transaction::Id,
    backend: Rc<dyn Backend>,
    crl: Rc<dyn crl::Crl>,
    net: Rc<dyn network::Messenger>,
    acceptor: paxos::Acceptor,
    txd: transaction::TransactionDescription,
    pre_tx_rebuilds: Vec<PreTransactionOpportunisticRebuild>,
    object_updates: HashMap<object::Id, ArcDataSlice>,
    oresolution: Option<bool>,
    ofinalized: Option<bool>,
    objects: HashMap<object::Id, store::TxStateRef>,
    pending_object_loads: usize,
    pending_object_commits: usize,
    last_prepare: LastPrepare,
    last_event: time::PreciseTime,
}

impl Tx {
    pub fn new(
        store_id: store::Id, 
        prepare: Prepare,
        object_locaters: &HashMap<object::Id, store::Pointer>,
        backend: &Rc<dyn Backend>,
        crl: &Rc<dyn crl::Crl>,
        net: &Rc<dyn network::Messenger>,) -> Tx {

        let pending_object_loads = object_locaters.len();

        Tx {
            txid: prepare.txd.id,
            backend: backend.clone(), 
            crl: crl.clone(),
            net: net.clone(),
            acceptor: paxos::Acceptor::new(store_id.pool_index, None),
            txd: prepare.txd,
            pre_tx_rebuilds: prepare.pre_tx_rebuilds,
            object_updates: prepare.object_updates,
            oresolution: None,
            ofinalized: None,
            objects: HashMap::new(),
            pending_object_loads,
            pending_object_commits: 0,
            last_prepare: LastPrepare {
                from: prepare.from,
                proposal_id: prepare.proposal_id,
                save_id: crl::TxSaveId(0)
            },
            last_event: time::PreciseTime::now(),
        }
    }

    pub fn resolved(&self) -> Option<bool> {
        return self.oresolution;
    }   

    pub fn finalized(&self) -> Option<bool> {
        return self.ofinalized;
    }

    fn update_last_event(&mut self) {
        self.last_event = time::PreciseTime::now();
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

    pub fn object_loaded(&mut self, state: &Rc<std::cell::RefCell<store::State>>) {

        let object_id = state.borrow().id;

        self.objects.insert(object_id, store::TxStateRef::new(state));

        self.pending_object_loads -= 1;

        if self.pending_object_loads == 0 {
            self.all_objects_loaded()
        }
    }

    
    pub fn all_objects_loaded(&mut self) {
        // Check to see if the tx is already resolved
    }

    pub fn crl_tx_save_complete(
        &mut self,
        save_id: crl::TxSaveId,
        success: bool) {

    }

    pub fn commit_complete(
        &mut self, 
        object_id: object::Id,
        result: Result<(), store::CommitError>) {
        // This can be less than the total number of objects referenced by this store
        // we skip ones with commit errors
    }

    
    pub fn receive_prepare(&mut self, msg: Prepare) {

        // Proposal ID 1 is always sent by the client initiating the transaction. We don't want to update
        // the timestamp for this since the client can't drive the transaction to completion and it'll
        // continually re-transmit the request to work around connection issues. Prepares sent by stores,
        // which will use a proposalId > 1 should up the the timestamp so we don't time out and also
        // attempt to drive the transaction forward.
        if msg.proposal_id.number != 1 {
            self.update_last_event();
        }

        match self.acceptor.receive_prepare(msg.proposal_id) {
            Ok(promise) => {
                // need CRL save before send. Mark prep response state here then actually send
                // message when we get save completion. Need to use a serial number here too
                // keep only the most recent. Use serial number in response to match response
                // with the reply
            },
            Err(nack) => {
                // this we can send straight away
            }
        }
    }

    pub fn receive_accept(&mut self, m: Accept) {

    }

    

}