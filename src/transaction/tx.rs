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
use crate::transaction::{Disposition, Status};

use super::messages::*;

struct LastPrepare {
    from: store::Id,
    proposal_id: paxos::ProposalId,
    response: PrepareResult,
    save_id: crl::TxSaveId
}

pub struct Tx {
    store_id: store::Id,
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
    disposition: transaction::Disposition,
    objects: HashMap<object::Id, store::TxStateRef>,
    pending_object_loads: usize,
    pending_object_commits: usize,
    last_prepare: LastPrepare,
    last_event: time::PreciseTime,
    save_object_updates: bool,
    last_crl_save_complete: Option<crl::TxSaveId>
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
            store_id,
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
            disposition: transaction::Disposition::Undetermined,
            objects: HashMap::new(),
            pending_object_loads,
            pending_object_commits: 0,
            last_prepare: LastPrepare {
                from: prepare.from,
                proposal_id: paxos::ProposalId{number: 0, peer: 0},
                response: PrepareResult::Nack(paxos::ProposalId{number: 0, peer: 0}),
                save_id: crl::TxSaveId(0)
            },
            last_event: time::PreciseTime::now(),
            save_object_updates: true,
            last_crl_save_complete: None
        }
    }

    fn update_last_event(&mut self) {
        self.last_event = time::PreciseTime::now();
    }

    pub fn object_loaded(&mut self, state: &Rc<std::cell::RefCell<store::State>>) {

        let object_id = state.borrow().id;

        self.objects.insert(object_id, store::TxStateRef::new(state));

        self.pending_object_loads -= 1;

        if self.pending_object_loads == 0 {
            self.all_objects_loaded()
        }
    }

    fn on_resolution(&mut self, value:bool) {
        if self.oresolution.is_none() {
            self.oresolution = Some(value);
        } 
    }
    
    fn all_objects_loaded(&mut self) {
        // Check to see if the tx is already resolved

        //if last_crl_save_complete, and ID matches self.last_prepare.save_id
        //send PrepareResponse directly
    }

    pub fn crl_tx_save_complete(
        &mut self,
        save_id: crl::TxSaveId,
        success: bool) {

        if success {
            self.last_crl_save_complete = Some(save_id);
        }

        if success && 
           save_id == self.last_prepare.save_id && 
           self.disposition != Disposition::Undetermined {

            let r = PrepareResponse {
                to: self.last_prepare.from,
                from: self.store_id,
                txid: self.txid,
                proposal_id: self.last_prepare.proposal_id,
                response: self.last_prepare.response.clone(),
                disposition: self.disposition
            };

            self.net.send_transaction_message(Message::PrepareResponse(r));
        }
    }

    pub fn commit_complete(
        &mut self, 
        object_id: object::Id,
        result: Result<(), store::CommitError>) {
        // This can be less than the total number of objects referenced by this store
        // we skip ones with commit errors
    }

    fn try_to_lock(&mut self) {

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
                match self.disposition {
                    Disposition::Undetermined => self.try_to_lock(),
                    Disposition::VoteAbort => self.try_to_lock(),
                    Disposition::VoteCommit => {
                        // Once we've decided to commit, we're committed to committing.
                        ()
                    }
                }

                let object_updates = if self.save_object_updates {
                    let v: Vec<transaction::ObjectUpdate> = self.object_updates.iter().map(
                    |(object_id, data)| {
                        transaction::ObjectUpdate {
                            object_id: *object_id,
                            data: data.clone()
                        }
                    }).collect();
                    Some(v)
                } else {
                    None
                };

                let save_id = self.last_prepare.save_id.next();

                self.last_prepare = LastPrepare {
                    from: msg.from,
                    proposal_id: msg.proposal_id,
                    response: PrepareResult::Promise(promise.last_accepted),
                    save_id
                };

                self.crl.save_transaction_state(
                    self.store_id, 
                    self.txid, 
                    self.txd.serialized_transaction_description.clone(),
                    object_updates,
                    self.disposition,
                    self.acceptor.persistent_state(),
                    save_id);
            },
            Err(nack) => {
                let r = PrepareResponse {
                    to: msg.from,
                    from: self.store_id,
                    txid: self.txid,
                    proposal_id: msg.proposal_id,
                    response: PrepareResult::Nack(nack.promised_proposal_id),
                    disposition: self.disposition
                };
                self.net.send_transaction_message(Message::PrepareResponse(r));
            }
        }
    }

    pub fn receive_accept(&mut self, m: Accept) {
        self.update_last_event();

        let response = match self.acceptor.receive_accept(m.proposal_id, m.value) {
            Ok(accepted) => AcceptResult::Accepted(accepted.proposal_value),
                
            Err(nack) => AcceptResult::Nack(nack.promised_proposal_id)
        };

        let r = AcceptResponse {
            to: m.from,
            from: self.store_id,
            txid: self.txid,
            proposal_id: m.proposal_id,
            response
        };

        self.net.send_transaction_message(Message::AcceptResponse(r));
    }

    pub fn receive_resolved(&mut self, m: Resolved) {
        self.update_last_event();
        self.on_resolution(m.value)
    }

    pub fn receive_finalized(&mut self, m: Finalized) {
        self.update_last_event();
        self.ofinalized = Some(m.value);
        self.on_resolution(m.value)
    }

    pub fn receive_heartbeat(&mut self, _: Heartbeat) {
        self.update_last_event();
    }

    pub fn receive_status_request(&mut self, m: StatusRequest) {
        let status = match self.oresolution {
            None => Status::Unresolved,
            Some(commit) => if commit {
                Status::Committed
            } else {
                Status::Aborted
            }
        };

        let r = StatusResponse {
            to: m.from,
            from: self.store_id,
            txid: self.txid,
            request_uuid: m.request_uuid,
            status,
            finalized: self.ofinalized.is_some()
        };

        self.net.send_transaction_message(Message::StatusResponse(r));
    }
}