use std::collections::{HashMap, HashSet};
use std::result;
use std::sync::Arc;

use crate::{ArcDataSlice, DataReader, SliceReader};
use crate::hlc;
use crate::object;
use crate::object::kv_encoding;
use crate::store::TxStateRef;
use crate::transaction;

use crate::transaction::requirements::*;

use super::checker::{ReqErr, get_objects_with_errors};

type Result = result::Result<(), ReqErr>;


pub struct ApplyError(object::Id, ReqErr);

pub struct SkippedObjects(HashSet<object::Id>);

pub fn apply_requirements(
    tx_id: transaction::Id,
    timestamp: hlc::Timestamp,
    requirements: &Vec<TransactionRequirement>,
    objects: &HashMap<object::Id, TxStateRef>,
    object_updates: &HashMap<object::Id, ArcDataSlice>) -> SkippedObjects {

    let oerrs = get_objects_with_errors(tx_id, requirements, objects, object_updates);


    SkippedObjects(oerrs)
}

pub fn apply_requirements_old(
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


    for r in requirements {
        match r {
            TransactionRequirement::LocalTime{..} => {
                () // Nothing to apply
            },
            TransactionRequirement::RevisionLock{..} => {
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
                match get_state(&pointer) {
                    Err(e) => errors.push(e),
                    Ok(obj) => {
                        match apply_kv_updates(tx_id, timestamp, obj, required_revision, key_requirements, 
                          object_updates.get(&pointer.id)) {
                            Err(e) => errors.push(ApplyError(pointer.id, e)),
                            Ok(_) => ()
                        }
                    }
                }
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

fn apply_kv_updates(
    tx_id: transaction::Id,
    timestamp: hlc::Timestamp,
    state: &TxStateRef,
    required_revision: &Option<object::Revision>,
    key_requirements: &Vec<KeyRequirement>,
    object_update: Option<&ArcDataSlice>) -> Result {

    let mut s = state.borrow_mut();

    if let Some(required_revision) = required_revision {
        if s.metadata.revision != * required_revision {
            return Err(ReqErr::RevisionMismatch)
        }
    }

    if object_update.is_none() {
        return Err(ReqErr::MissingObjectUpdate)
    }

    let cvt = |key: &Key| -> object::Key {
        object::Key::from_bytes(key.as_bytes())
    };

    let check_lock = |e: &object::KVEntry| -> Result {
        if let Some(locked_tx_id) = e.locked_to_transaction {
            if locked_tx_id != tx_id {
                return Err(ReqErr::TransactionCollision)
            }
        }
        Ok(())
    };

    if let Some(kv) = s.kv_state() {
        for r in key_requirements {
            match r {
                KeyRequirement::Exists{key} => {
                    let k = &cvt(key);

                    match kv.content.get(&k) {
                        None => return Err(ReqErr::KeyExistenceError),
                        Some(s) => check_lock(s)?
                    }
                },
                KeyRequirement::MayExist{key} => {
                    let k = &cvt(key);

                    match kv.content.get(&k) {
                        None => {
                            if kv.no_existence_locks.contains(&k) {
                                return Err(ReqErr::TransactionCollision);
                            }
                        },
                        Some(s) => check_lock(s)? 
                    }
                },
                KeyRequirement::DoesNotExist{key} => {
                    let k = &cvt(key);

                    match kv.content.get(&k) {
                        Some(_) => return Err(ReqErr::KeyExistenceError),
                        None => {
                            if kv.no_existence_locks.contains(&k) {
                                return Err(ReqErr::TransactionCollision);
                            }
                        }
                    }
                },
                KeyRequirement::TimestampLessThan{key, timestamp} => {
                    match kv.content.get(&cvt(key)) {
                        None => return Err(ReqErr::KeyExistenceError),
                        Some(s) => {
                            if s.timestamp > *timestamp {
                                return Err(ReqErr::KeyTimestampError)
                            }
                            check_lock(s)?
                        }
                    }
                },
                KeyRequirement::TimestampGreaterThan{key, timestamp} => {
                    match kv.content.get(&cvt(key)) {
                        None => return Err(ReqErr::KeyExistenceError),
                        Some(s) => {
                            if s.timestamp < *timestamp {
                                return Err(ReqErr::KeyTimestampError)
                            }
                            check_lock(s)?
                        }
                    }
                },
                KeyRequirement::TimestampEquals{key, timestamp} => {
                    match kv.content.get(&cvt(key)) {
                        None => return Err(ReqErr::KeyExistenceError),
                        Some(s) => {
                            if s.timestamp != *timestamp {
                                return Err(ReqErr::KeyTimestampError)
                            }
                            check_lock(s)?
                        }
                    }
                },
            }
        }

        let data = SliceReader::new(object_update.unwrap());
        let tx_revision = object::Revision(tx_id.0);
        let kv_ops = kv_encoding::decode_operations(data.remaining_bytes(),
            timestamp, tx_revision);

        for op in kv_ops {
            match op.operation {
                object::KVOpCode::SetMinCode      => {
                    kv.min = Some(op.key);
                },
                object::KVOpCode::SetMaxCode      => {
                    kv.max = Some(op.key);
                },
                object::KVOpCode::SetLeftCode     => {
                    kv.left = Some(op.key);
                },
                object::KVOpCode::SetRightCode    => {
                    kv.right = Some(op.key);
                },
                object::KVOpCode::InsertCode      => {
                    kv.content.insert(op.key, object::KVEntry {
                        value: op.value,
                        revision: op.revision,
                        timestamp: op.timestamp,
                        locked_to_transaction: None
                    });
                },
                object::KVOpCode::DeleteCode      => {
                    kv.content.remove(&op.key);
                },
                object::KVOpCode::DeleteMinCode   => {
                    kv.min = None;
                },
                object::KVOpCode::DeleteMaxCode   => {
                    kv.max = None;
                },
                object::KVOpCode::DeleteLeftCode  => {
                    kv.left = None;
                },
                object::KVOpCode::DeleteRightCode => {
                    kv.right = None;
                },
            }
        }

        Ok(())
    } else {
        Err(ReqErr::ObjectTypeError)
    }
}