use std::rc::Rc;

use crate::transaction;
use crate::network;
use crate::store;

pub struct Driver {
    store_id: store::Id,
    net: Rc<dyn network::Messenger>,
}

impl Driver {
    pub fn new(store_id: store::Id, 
        net: &Rc<dyn network::Messenger>, 
        prepare: &transaction::messages::Prepare) -> Driver {

        Driver {
            store_id,
            net: net.clone()
        }
    }

    pub fn recover(tx: &transaction::tx::Tx) -> Driver {
        Driver {
            store_id: tx.get_store_id(),
            net: tx.get_messenger().clone()
        }
    }

    pub fn heartbeat(&mut self) {

    }

    pub fn receive_prepare_response(&mut self, pr: &transaction::messages::PrepareResponse) {

    }

    pub fn receive_transaction_message(&mut self, msg: &transaction::messages::Message) {

    }
}