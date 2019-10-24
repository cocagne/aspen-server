use std::cell::RefCell;
use std::rc;

use crate::object;
use crate::store;
use crate::transaction;

use crate::network::client_messages;
use crate::network::*;

pub struct ReadResponse {
    pub client_id: ClientId,
    pub request_id: RequestId,
    pub object_id: object::Id,
    pub result: Result<store::ReadState, store::ReadError>
}

pub struct NullMessengerState {
    pub reads: Vec<ReadResponse>,
    pub txs: Vec<transaction::Message>,
    pub clis: Vec<client_messages::Message>
}

pub struct NullMessenger {
    state: rc::Rc<RefCell<NullMessengerState>>
}

impl NullMessenger {
    pub fn new() -> (rc::Rc<RefCell<NullMessengerState>>, rc::Rc<dyn Messenger>) {
        let s = rc::Rc::new( RefCell::new(NullMessengerState {
            reads: Vec::new(),
            txs:  Vec::new(),
            clis: Vec::new(),
        }));

        let n = rc::Rc::new( NullMessenger{
            state: s.clone()
        });

        (s, n)
    }
}

impl Messenger for NullMessenger {
    fn send_read_response(
        &self,
        client_id: ClientId,
        request_id: RequestId,
        object_id: object::Id,
        result: Result<store::ReadState, store::ReadError>) {
        
        self.state.borrow_mut().reads.push( ReadResponse {
            client_id,
            request_id,
            object_id,
            result
        });
    }

    fn send_transaction_message(&self, msg: transaction::Message) {
        self.state.borrow_mut().txs.push(msg);
    }

    fn send_client_message(&self, msg: client_messages::Message) {
        self.state.borrow_mut().clis.push(msg);
    }
}