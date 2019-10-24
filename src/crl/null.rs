
use std::cell::RefCell;
use std::rc::Rc;

use super::ArcDataSlice;
use super::hlc;
use super::object;
use super::paxos;
use super::store;
use super::transaction;

use super::*;

pub struct NullCRLState {
    pub state_saves: usize,
    pub alloc_saves: usize
}

pub struct NullCRL {
   pub state: Rc<RefCell<NullCRLState>>
}

impl NullCRL {
    pub fn new() -> (Rc<RefCell<NullCRLState>>, Rc<dyn crate::crl::Crl>) {
        let state = Rc::new(RefCell::new(NullCRLState {
            state_saves: 0,
            alloc_saves: 0
        }));

        let c = Rc::new(NullCRL {
           state: state.clone()
        });

        (state, c)
    }
}

impl crate::crl::Crl for NullCRL {
    fn get_full_recovery_state(
        &self, 
        _store_id: store::Id) -> (Vec<TransactionRecoveryState>, Vec<AllocationRecoveryState>) {
        (Vec::new(), Vec::new())
    }

    fn save_transaction_state(
        &self,
        _store_id: store::Id,
        _transaction_id: transaction::Id,
        _serialized_transaction_description: ArcDataSlice,
        _object_updates: Option<Vec<transaction::ObjectUpdate>>,
        _tx_disposition: transaction::Disposition,
        _paxos_state: paxos::PersistentState,
        _save_id: TxSaveId
    ) {
        self.state.borrow_mut().state_saves += 1;
    }

    fn drop_transaction_object_data(
        &self,
        _store_id: store::Id,
        _transaction_id: transaction::Id
    ) {}

    fn delete_transaction_state(
        &self,
        _store_id: store::Id,
        _transaction_id: transaction::Id
    ) {}

    fn save_allocation_state(
        &self,
        _store_id: store::Id,
        _store_pointer: store::Pointer,
        _id: object::Id,
        _kind: object::Kind,
        _size: Option<u32>,
        _data: ArcDataSlice,
        _refcount: object::Refcount,
        _timestamp: hlc::Timestamp,
        _allocation_transaction_id: transaction::Id,
        _serialized_revision_guard: ArcDataSlice
    ) {
        self.state.borrow_mut().alloc_saves += 1;
    }

    fn delete_allocation_state(
        &self,
        _store_id: store::Id, 
        _allocation_transaction_id: transaction::Id){}
}