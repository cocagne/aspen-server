use std::collections::HashMap;

use crate::object;
use crate::store::TxStateRef;
use crate::transaction;

use crate::transaction::requirements::*;


pub fn lock_requirements(
    tx_id: transaction::Id,
    requirements: &Vec<TransactionRequirement>,
    objects: &mut HashMap<object::Id, TxStateRef>) {
    
    for r in requirements {
        let optr = match r {
            TransactionRequirement::LocalTime{..} => None,
            TransactionRequirement::RevisionLock{pointer, ..} => Some(pointer),
            TransactionRequirement::VersionBump{pointer, ..} => Some(pointer),
            TransactionRequirement::RefcountUpdate{pointer, ..} => Some(pointer),
            TransactionRequirement::DataUpdate{pointer, ..} => Some(pointer),
            TransactionRequirement::KeyValueUpdate{pointer, required_revision, full_content_lock, key_requirements} => {
                lock_kv_requirements(tx_id, objects.get_mut(&pointer.id).unwrap(), &key_requirements);
                required_revision.map( |_| pointer)
            },
        };

        if let Some(pointer) = optr {
            objects.get(&pointer.id).unwrap().borrow_mut().locked_to_transaction = Some(tx_id);
        }
    }
}


fn lock_kv_requirements(
    tx_id: transaction::Id,
    state: &mut TxStateRef,
    key_requirements: &Vec<KeyRequirement>) {

    let mut s = state.borrow_mut();

    let mut object_lock: Option<transaction::Id> = None;

    {
        let kv = s.kv_state.as_mut().unwrap();

        for r in key_requirements {
            match r {
                KeyRequirement::Exists{key} => {
                    kv.content.get_mut(&key).unwrap().locked_to_transaction = Some(tx_id);
                },
                KeyRequirement::MayExist{key} => {
                    if let Some(kvs) = kv.content.get_mut(&key) {
                        kvs.locked_to_transaction = Some(tx_id);
                    }
                },
                KeyRequirement::DoesNotExist{key} => {
                    kv.no_existence_locks.insert(key.clone());
                },
                KeyRequirement::TimestampLessThan{key, ..} => {
                    kv.content.get_mut(&key).unwrap().locked_to_transaction = Some(tx_id)
                },
                KeyRequirement::TimestampGreaterThan{key, ..} => {
                    kv.content.get_mut(&key).unwrap().locked_to_transaction = Some(tx_id)
                },
                KeyRequirement::TimestampEquals{key, ..} => {
                    kv.content.get_mut(&key).unwrap().locked_to_transaction = Some(tx_id)
                },
                KeyRequirement::KeyRevision{key, ..} => {
                    kv.content.get_mut(&key).unwrap().locked_to_transaction = Some(tx_id)
                },
                KeyRequirement::KeyObjectRevision{..} => {
                    object_lock = Some(tx_id)
                },
                KeyRequirement::WithinRange{..} => {
                    object_lock = Some(tx_id)
                }

            }
        }
    }

    s.locked_to_transaction = object_lock
}

pub fn unlock_requirements(
    requirements: &Vec<TransactionRequirement>,
    objects: &mut HashMap<object::Id, TxStateRef>) {
    
    for r in requirements {
        let optr = match r {
            TransactionRequirement::LocalTime{..} => None,
            TransactionRequirement::RevisionLock{pointer, ..} => Some(pointer),
            TransactionRequirement::VersionBump{pointer, ..} => Some(pointer),
            TransactionRequirement::RefcountUpdate{pointer, ..} => Some(pointer),
            TransactionRequirement::DataUpdate{pointer, ..} => Some(pointer),
            TransactionRequirement::KeyValueUpdate{pointer, required_revision, full_content_lock, key_requirements} => {
                unlock_kv_requirements(objects.get_mut(&pointer.id).unwrap(), &key_requirements);
                required_revision.map( |_| pointer)
            },
        };

        if let Some(pointer) = optr {
            objects.get(&pointer.id).unwrap().borrow_mut().locked_to_transaction = None;
        }
    }
}


fn unlock_kv_requirements(
    state: &mut TxStateRef,
    key_requirements: &Vec<KeyRequirement>) {

    let mut s = state.borrow_mut();
    let mut unlock_object = false;

    {
        let kv = s.kv_state.as_mut().unwrap();

        for r in key_requirements {
            match r {
                KeyRequirement::Exists{key} => {
                    kv.content.get_mut(&key).unwrap().locked_to_transaction = None;
                },
                KeyRequirement::MayExist{key} => {
                    if let Some(s) = kv.content.get_mut(&key) {
                        s.locked_to_transaction = None;
                    }
                },
                KeyRequirement::DoesNotExist{key} => {
                    kv.no_existence_locks.remove(&key);
                },
                KeyRequirement::TimestampLessThan{key, ..} => {
                    kv.content.get_mut(&key).unwrap().locked_to_transaction = None
                },
                KeyRequirement::TimestampGreaterThan{key, ..} => {
                    kv.content.get_mut(&key).unwrap().locked_to_transaction = None
                },
                KeyRequirement::TimestampEquals{key, ..} => {
                    kv.content.get_mut(&key).unwrap().locked_to_transaction = None
                },
                KeyRequirement::KeyRevision{key, ..} => {
                    kv.content.get_mut(&key).unwrap().locked_to_transaction = None
                },
                KeyRequirement::KeyObjectRevision{..} => {
                    unlock_object = true;
                },
                KeyRequirement::WithinRange{..} => {
                    unlock_object = true;
                }
            }
        }
    }

    if unlock_object {
        s.locked_to_transaction = None;
    }

}