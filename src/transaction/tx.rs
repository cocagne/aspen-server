use std::collections::HashMap;
use std::rc::Rc;

use time;

use crate::data::ArcDataSlice;
use crate::network;
use crate::object;
use crate::paxos;
use crate::store;
use crate::transaction;

use super::messages::*;
/*
Tx steps
    1. Create on Prepare, PrepareResponse, or Accept
    2. On prepare, start loading all objects
    3. when load complete
*/

pub struct Tx {
    txid: transaction::Id,
    acceptor: paxos::Acceptor,
    txd: transaction::TransactionDescription,
    pre_tx_rebuilds: Vec<PreTransactionOpportunisticRebuild>,
    object_updates: HashMap<object::Id, ArcDataSlice>,
    oresolution: Option<bool>,
    ofinalized: Option<bool>,
    pub objects: HashMap<object::Id, store::TxStateRef>,
    pub pending_object_loads: usize,
    last_event: time::PreciseTime,
}

impl Tx {
    pub fn new(store_id: store::Id, prepare: Prepare) -> Tx {
        Tx {
            txid: prepare.txd.id,
            acceptor: paxos::Acceptor::new(store_id.pool_index, None),
            txd: prepare.txd,
            pre_tx_rebuilds: prepare.pre_tx_rebuilds,
            object_updates: prepare.object_updates,
            oresolution: None,
            ofinalized: None,
            objects: HashMap::new(),
            pending_object_loads: 0,
            last_event: time::PreciseTime::now(),
        }
    }

    pub fn resolved(&self) -> Option<bool> {
        return self.oresolution;
    }   

    pub fn finalized(&self) -> Option<bool> {
        return self.ofinalized;
    }

    pub fn objects_loaded(&mut self) {

    }

    // If 1st prepare, fill out member variables with content.
    // Need to iterate over all objects hosted by this store and either put a TxStateRef
    // in the self.objects map or do a read and put the object ID in teh remaining_objects
    // add objects_loaded() method that accepts a net param and sends a prepare_result
    // ... may want to delay calling receive_prepare until all objects are loaded
    //     could have Option<Prepare> as member.
    pub fn receive_prepare(
        &mut self,
        m: Prepare, 
        net: &Rc<dyn network::Messenger>,
        ) {
        match self.acceptor.receive_prepare(m.proposal_id) {
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

    pub fn receive_accept(
        &mut self,
        m: Accept,
        net: &Rc<dyn network::Messenger>
    ) {

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