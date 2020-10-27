use std::rc::Rc;
use std::collections::{ HashSet, HashMap };

use crate::transaction;
use crate::network;
use crate::store;
use crate::paxos;
use crate::object;

pub struct Driver {
    store_id: store::Id,
    net: Rc<dyn network::Messenger>,
    txd: transaction::TransactionDescription,
    proposer: paxos::proposer::Proposer,
    learner: paxos::learner::Learner,
    valid_acceptor_set: HashSet<u8>,
    all_objects: HashSet<object::Pointer>,
    primary_data_stores: HashSet<store::Id>,
    all_data_stores: HashSet<store::Id>,
    resolved: bool,
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
        prepare: &transaction::messages::Prepare) -> Driver {

        Driver::create(store_id, net, &prepare.txd)
    }

    pub fn recover(tx: &transaction::tx::Tx) -> Driver {
        Driver::create(tx.get_store_id(), tx.get_messenger(), tx.get_txd())
    }

    fn create(store_id: store::Id, net: &Rc<dyn network::Messenger>, txd: &transaction::TransactionDescription) -> Driver {
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

    fn receive_prepare(&mut self, p: &transaction::messages::Prepare) {

    }

    // Called both by the receive_transaction_message function as well as directly invoked by
    // the manager immediately after creation to deliver cached prepare responses that arrive
    // before the Prepare message indicating that this node is the dedicated driver.
    pub fn receive_prepare_response(&mut self, pr: &transaction::messages::PrepareResponse) {

    }

    fn receive_accept_response(&mut self, ar: &transaction::messages::AcceptResponse) {

    }

    fn receive_resolved(&mut self, r: &transaction::messages::Resolved) {

    }

    fn receive_finalized(&mut self, f: &transaction::messages::Finalized) {
        
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
            transaction::messages::Message::Heartbeat(m) => self.receive_heartbeat(&m),
        
            _ => () // ignore the rest
        }
    }
}