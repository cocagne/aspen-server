
use std::rc::Rc;

use crate::transaction;
use crate::network;
use crate::store;
use crate::object;

pub trait Finalizer {

    fn update_commit_errors(&mut self, store_id: store::Id, errors: &Vec<object::Id>);

    fn finalization_complete(&mut self);

}

pub trait FinalizerFactory {
    fn create(&self, 
              txd: &transaction::TransactionDescription, 
              net: &Rc<dyn network::Messenger>) -> Box<dyn Finalizer>;
}