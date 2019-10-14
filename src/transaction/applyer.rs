use std::collections::HashMap;
use std::result;
use std::sync::Arc;

use crate::{ArcDataSlice, DataMut};
use crate::hlc;
use crate::object;
use crate::store::TxStateRef;
use crate::transaction;

use crate::transaction::requirements::*;

use super::checker::ReqErr;

type Result = result::Result<(), ReqErr>;


pub struct ApplyError(object::Id, ReqErr);

pub fn apply_requirements(
    tx_id: transaction::Id,
    timestamp: hlc::Timestamp,
    requirements: &Vec<TransactionRequirement>,
    objects: &HashMap<object::Id, TxStateRef>,
    object_updates: &HashMap<object::Id, ArcDataSlice>) -> Vec<ApplyError> {

    let mut errors: Vec<ApplyError> = Vec::new();
    
    let get_state = |ptr: &object::Pointer| -> result::Result<&TxStateRef, ApplyError> {
        match objects.get(&ptr.id) {
            Some(o) => {
                match o.borrow().locked_to_transaction {
                    None => Ok(o),
                    Some(id) => {
                        if id == tx_id {
                            Ok(o)
                        } else {
                            Err(ApplyError(ptr.id, ReqErr::TransactionCollision))
                        }
                    }
                }
            },
            None => Err(ApplyError(ptr.id, ReqErr::MissingObject))
        }
    };

    let mut ou_iter = object_updates.iter();

    for r in requirements {
        match r {
            TransactionRequirement::LocalTime{requirement} => {
                () // Nothing to apply
            },
            TransactionRequirement::RevisionLock{pointer, required_revision} => {
                () // Nothing to apply
            },
            TransactionRequirement::VersionBump{pointer, required_revision} => {
                match get_state(&pointer) {
                    Err(e) => errors.push(e),
                    Ok(obj) => {
                        match apply_revision(tx_id, timestamp, obj, required_revision) {
                            Err(e) => errors.push(ApplyError(pointer.id, e)),
                            Ok(_) => ()
                        }
                    }
                }
            },
            TransactionRequirement::RefcountUpdate{pointer, required_refcount, new_refcount} => {
                match get_state(&pointer) {
                    Err(e) => errors.push(e),
                    Ok(obj) => {
                        match apply_refcount(tx_id, timestamp, obj, required_refcount, new_refcount) {
                            Err(e) => errors.push(ApplyError(pointer.id, e)),
                            Ok(_) => ()
                        }
                    }
                }
            },
            TransactionRequirement::DataUpdate{pointer, required_revision, operation} => {
                 match get_state(&pointer) {
                    Err(e) => errors.push(e),
                    Ok(obj) => {
                        match apply_data_update(tx_id, timestamp, obj, required_revision, 
                          operation, object_updates.get(&pointer.id)) {
                            Err(e) => errors.push(ApplyError(pointer.id, e)),
                            Ok(_) => ()
                        }
                    }
                }
            },
            TransactionRequirement::KeyValueUpdate{pointer, required_revision, key_requirements} => {
                 //check_kv_requirements(tx_id, get_state(&pointer)?, required_revision, key_requirements)?
            },
        }
    }

    errors
}

fn apply_revision(
    tx_id: transaction::Id,
    timestamp: hlc::Timestamp,
    state: &TxStateRef, 
    required_revision: &object::Revision) -> Result {

    let mut mstate = state.borrow_mut();

    if let Some(locked_tx) = mstate.locked_to_transaction {
        if locked_tx != tx_id {
            return Err(ReqErr::TransactionCollision)
        }
    }

    if mstate.metadata.revision == *required_revision {
        mstate.metadata.revision = object::Revision(tx_id.0);
        mstate.metadata.timestamp = timestamp;
        Ok(())
    } else {
        Err(ReqErr::RevisionMismatch)
    }
}

fn apply_refcount(
    tx_id: transaction::Id,
    timestamp: hlc::Timestamp,
    state: &TxStateRef,
    required_refcount: &object::Refcount,
    new_refcount: &object::Refcount) -> Result {

    let mut mstate = state.borrow_mut();

    if let Some(locked_tx) = mstate.locked_to_transaction {
        if locked_tx != tx_id {
            return Err(ReqErr::TransactionCollision)
        }
    }

    if mstate.metadata.refcount == *required_refcount {
        mstate.metadata.refcount = *new_refcount;
        mstate.metadata.timestamp = timestamp;
        Ok(())
    } else {
        Err(ReqErr::RefcountMismatch)
    }
}

fn apply_data_update(
    tx_id: transaction::Id,
    timestamp: hlc::Timestamp,
    state: &TxStateRef, 
    required_revision: &object::Revision,
    operation: &object::DataUpdateOperation,
    object_update: Option<&ArcDataSlice>) -> Result {

    let mut mstate = state.borrow_mut();

    if let Some(locked_tx) = mstate.locked_to_transaction {
        if locked_tx != tx_id {
            return Err(ReqErr::TransactionCollision)
        }
    }

    if mstate.metadata.revision == *required_revision {

        if let Some(data) = object_update {
            mstate.metadata.revision = object::Revision(tx_id.0);
            mstate.metadata.timestamp = timestamp;

            match operation {
                object::DataUpdateOperation::Overwrite => {
                    let mut v = Vec::with_capacity(data.len());
                    v.copy_from_slice(data.as_bytes());
                    mstate.data = Arc::new(v);
                },
                object::DataUpdateOperation::Append => {
                    let mut v = Vec::with_capacity(mstate.data.len() + data.len());
                    v[0..mstate.data.len()].copy_from_slice(&mstate.data[..]);
                    v[mstate.data.len()..].copy_from_slice(data.as_bytes());
                    mstate.data = Arc::new(v);
                }
            }

            Ok(())
        } else {
            Err(ReqErr::MissingObjectUpdate)
        }
    } else {
        Err(ReqErr::RevisionMismatch)
    }
}

