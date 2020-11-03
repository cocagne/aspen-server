use std::rc::Rc;
use std::collections::{ HashSet, HashMap };

use crate::transaction;
use crate::network;
use crate::store;
use crate::paxos;
use crate::object;
use crate::finalizer;

pub struct Driver {
    store_id: store::Id,
    net: Rc<dyn network::Messenger>,
    finalizer_factory: Rc<dyn finalizer::FinalizerFactory>,
    txd: transaction::TransactionDescription,
    proposer: paxos::proposer::Proposer,
    learner: paxos::learner::Learner,
    valid_acceptor_set: HashSet<u8>,
    all_objects: HashSet<object::Pointer>,
    primary_data_stores: HashSet<store::Id>,
    all_data_stores: HashSet<store::Id>,
    resolved: bool,
    finalizer: Option<Box<dyn finalizer::Finalizer>>,
    finalized: bool,
    peer_dispositions: HashMap<store::Id, transaction::Disposition>,
    accepted_peers: HashSet<store::Id>,
    known_resolved: HashSet<store::Id>,
    resolved_value: bool,
    commit_errors: HashMap<store::Id, Vec<object::Id>>,
    should_send_accept_messages: bool,
    targeted_prepare: Option<store::Id>,
    client_resolved: Option<transaction::messages::Resolved>,
    client_finalized: Option<transaction::messages::Finalized>,
    tx_messages: Option<Vec<transaction::messages::Message>>
}

impl Driver {
    pub fn new(store_id: store::Id, 
        net: &Rc<dyn network::Messenger>,
        finalizer_factory: &Rc<dyn finalizer::FinalizerFactory>,
        prepare: &transaction::messages::Prepare) -> Driver {

        Driver::create(store_id, net, finalizer_factory, &prepare.txd)
    }

    pub fn recover(tx: &transaction::tx::Tx, finalizer_factory: &Rc<dyn finalizer::FinalizerFactory>) -> Driver {
        let mut d = Driver::create(tx.get_store_id(), tx.get_messenger(), finalizer_factory, tx.get_txd());
        d.proposer.set_local_proposal(false); // Alawys start with attemtping an abort when recovering
        d.send_prepare_messages();
        d
    }

    fn create(store_id: store::Id, 
              net: &Rc<dyn network::Messenger>,
              finalizer_factory: &Rc<dyn finalizer::FinalizerFactory>,
              txd: &transaction::TransactionDescription) -> Driver {
        let all_objects = txd.all_objects();
        let mut all_data_stores = HashSet::new();
        for o in all_objects {
            for id in o.get_store_pointers_set() {
                all_data_stores.insert(id);
            }
        }
        Driver {
            store_id: store_id,
            net: net.clone(),
            proposer: paxos::proposer::Proposer::new(store_id.pool_index, 
                                                     txd.primary_object.ida.width(), 
                                                     txd.primary_object.ida.write_threshold()),
            learner: paxos::learner::Learner::new(txd.primary_object.ida.width(), txd.primary_object.ida.write_threshold()),
            valid_acceptor_set: txd.primary_object.get_valid_acceptor_set(),
            all_objects: txd.all_objects(),
            primary_data_stores: txd.primary_object.get_store_pointers_set(),
            all_data_stores: all_data_stores,
            resolved: false,
            finalizer: None,
            finalizer_factory: finalizer_factory.clone(),
            finalized: false,
            peer_dispositions: HashMap::new(),
            accepted_peers: HashSet::new(),
            known_resolved: HashSet::new(),
            resolved_value: false,
            commit_errors: HashMap::new(),
            should_send_accept_messages: false,
            targeted_prepare: None,
            client_resolved: None,
            client_finalized: None,
            tx_messages: None,
            txd: txd.clone()
        }
    }

    fn is_valid_acceptor(&self, ds: &store::Id) -> bool {
        ds.pool_uuid == self.txd.primary_object.pool_id.0 && self.valid_acceptor_set.contains(&ds.pool_index)
    }

    pub fn heartbeat(&mut self) {

    }

    fn on_nack(&mut self, pid: paxos::ProposalId) {
        if pid.number > self.proposer.current_proposal_id().number {
            self.proposer.update_highest_proposal_id(pid);
            self.peer_dispositions.clear();
            self.accepted_peers.clear();
            self.proposer.next_round();
        }
    }

    fn on_promise(&mut self, 
                  pr: &transaction::messages::PrepareResponse, 
                  last_accepted: Option<(paxos::ProposalId, bool)>) {

        self.peer_dispositions.insert(pr.from, pr.disposition);

        if self.is_valid_acceptor(&pr.from) {
            self.proposer.receive_promise(pr.from.pool_index, pr.proposal_id, last_accepted);
        }

        if self.proposer.prepare_quorum_reached() {
            let mut already_had_local_proposal = self.proposer.have_local_proposal();

            if ! self.proposer.have_local_proposal() {
                already_had_local_proposal = false;

                let mut can_commit_transaction = true;

                for ptr in &self.all_objects {
                    let mut num_replies = 0;
                    let mut num_commit_votes = 0;
                    let mut num_abort_votes = 0;
                    let mut can_commit_object = true;

                    for sp in &ptr.store_pointers {
                        let store_id = store::Id {
                            pool_uuid: ptr.pool_id.0,
                            pool_index: sp.pool_index()
                        };
                        if let Some(disposition) = self.peer_dispositions.get(&store_id) {
                            num_replies += 1;
                            match disposition {
                                transaction::Disposition::VoteCommit => num_commit_votes += 1,
                                _ => num_abort_votes += 1 // TODO: We can be quite a bit smarter about choosing when we must abort
                            }
                        }
                    }

                    if num_replies < ptr.ida.write_threshold() {
                        return
                    }

                    if num_replies != ptr.ida.width() && 
                       num_commit_votes < ptr.ida.write_threshold() && 
                       ptr.ida.width() - num_abort_votes >= ptr.ida.write_threshold() {
                        return
                    }

                    if ptr.ida.width() - num_abort_votes < ptr.ida.write_threshold() {
                        can_commit_object = false
                    }

                    // Once can_commit_transaction flips to false, it stays there
                    can_commit_transaction = can_commit_transaction && can_commit_object
                }

                // If we get here, we've made our decision
                self.proposer.set_local_proposal(can_commit_transaction);

                self.should_send_accept_messages = true;
            }

            if self.proposer.have_local_proposal() && ! already_had_local_proposal {
                self.send_accept_messages()
            }
        }
    }

    fn send_prepare_messages(&mut self) {
        for store_id in &self.all_data_stores {
            
            let msg = transaction::messages::Message::Prepare(transaction::messages::Prepare{
                to: *store_id,
                from: self.store_id,
                proposal_id: self.proposer.current_proposal_id(),
                txd: self.txd.clone(),
                object_updates: HashMap::new(),
                pre_tx_rebuilds: Vec::new()
            });
            
            self.net.send_transaction_message(msg);
        }
    }

    fn send_accept_messages(&mut self) {
        for store_id in &self.all_data_stores {
            
            let msg = transaction::messages::Message::Accept(transaction::messages::Accept{
                to: *store_id,
                from: self.store_id,
                proposal_id: self.proposer.current_proposal_id(),
                txid: self.txd.id,
                value: self.proposer.proposal_value().unwrap()
            });
            
            self.net.send_transaction_message(msg);
        }
    }

    fn receive_prepare(&mut self, p: &transaction::messages::Prepare) {
        self.proposer.update_highest_proposal_id(p.proposal_id);
    }

    // Called both by the receive_transaction_message function as well as directly invoked by
    // the manager immediately after creation to deliver cached prepare responses that arrive
    // before the Prepare message indicating that this node is the dedicated driver.
    pub fn receive_prepare_response(&mut self, pr: &transaction::messages::PrepareResponse) {
        if pr.proposal_id != self.proposer.current_proposal_id() {
            return // Response isn't for the current round. Ignore it
        }

        match pr.response {
            transaction::messages::PrepareResult::Nack(pid) => self.on_nack(pid),
            transaction::messages::PrepareResult::Promise(last_accepted) => self.on_promise(pr, last_accepted)
        }
    }

    fn receive_accept_response(&mut self, ar: &transaction::messages::AcceptResponse) {
        if ar.proposal_id != self.proposer.current_proposal_id() {
            return
        }
        if ! self.is_valid_acceptor(&ar.from) {
            return
        }
        match ar.response {
            transaction::messages::AcceptResult::Nack(proposal_id) => self.on_nack(proposal_id),
            transaction::messages::AcceptResult::Accepted(value) => {
                self.accepted_peers.insert(ar.from);
                let was_resolved = self.learner.final_value().is_some();
                self.learner.receive_accepted(ar.from.pool_index, ar.proposal_id, value);
                if !was_resolved {
                    if let Some(final_value) = self.learner.final_value() {
                        self.on_resolution(final_value)
                    }
                }
            }

        }
    }

    // Note that this can be called multiple times, once for each Paxos driven to 
    // resolution.
    fn on_resolution(&mut self, committed: bool) {

        self.resolved = true;

        if committed && self.finalizer.is_none() {
            self.finalizer = Some(self.finalizer_factory.create(&self.txd, &self.net));
        }

        for store_id in &self.all_data_stores {
            
            let msg = transaction::messages::Message::Resolved(transaction::messages::Resolved{
                to: *store_id,
                from: self.store_id,
                txid: self.txd.id,
                value: self.proposer.proposal_value().unwrap()
            });
            
            self.net.send_transaction_message(msg);
        }        
    }

    fn receive_committed(&mut self, r: &transaction::messages::Committed) {
        if let Some(finalizer) = &mut self.finalizer {
            finalizer.update_commit_errors(r.from, &r.object_commit_errors);
        }
    }

    fn receive_resolved(&mut self, r: &transaction::messages::Resolved) {
        if ! self.resolved {
            self.on_resolution(r.value)
        }
    }

    fn receive_finalized(&mut self, f: &transaction::messages::Finalized) {
        if ! self.resolved {
            self.on_resolution(f.value)
        }

        if let Some(finalizer) = self.finalizer {
            finalizer.finalization_complete();
        }
    }

    fn receive_heartbeat(&mut self, h: &transaction::messages::Heartbeat) {
        
    }

    pub fn receive_transaction_message(&mut self, msg: &transaction::messages::Message) {
        match msg {
            transaction::messages::Message::Prepare(m) => self.receive_prepare(&m),
            transaction::messages::Message::PrepareResponse(m) => self.receive_prepare_response(&m),
            transaction::messages::Message::AcceptResponse(m) => self.receive_accept_response(&m),
            transaction::messages::Message::Resolved(m) => self.receive_resolved(&m),
            transaction::messages::Message::Finalized(m) => self.receive_finalized(&m),
            transaction::messages::Message::Committed(m) => self.receive_committed(&m),
            transaction::messages::Message::Heartbeat(m) => self.receive_heartbeat(&m),
        
            _ => () // ignore the rest
        }
    }
}