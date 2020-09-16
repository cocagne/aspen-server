use std::collections::{HashMap, HashSet};
use std::result;

use crate::ArcDataSlice;
use crate::hlc;
use crate::object;
use crate::store::TxStateRef;
use crate::transaction;

use crate::transaction::requirements::*;

type Result = result::Result<(), ReqErr>;

#[derive(Debug, PartialEq, Eq)]
pub enum ReqErr {
    TransactionCollision,
    LocalTimeError,
    MissingObject,
    MissingObjectUpdate,
    ObjectTypeError,
    RevisionMismatch,
    RefcountMismatch,
    KeyTimestampError,
    KeyExistenceError
}

pub fn check_requirements(
    tx_id: transaction::Id,
    requirements: &Vec<TransactionRequirement>,
    objects: &HashMap<object::Id, TxStateRef>,
    object_updates: &HashMap<object::Id, ArcDataSlice>) -> Result {
    
    let get_state = |ptr: &object::Pointer| -> result::Result<&TxStateRef, ReqErr> {
        match objects.get(&ptr.id) {
            Some(o) => {
                match o.borrow().locked_to_transaction {
                    None => Ok(o),
                    Some(id) => {
                        if id == tx_id {
                            Ok(o)
                        } else {
                            Err(ReqErr::TransactionCollision)
                        }
                    }
                }
            },
            None => Err(ReqErr::MissingObject)
        }
    };

    for r in requirements {
        match r {
            TransactionRequirement::LocalTime{requirement} => {
                check_localtime(requirement)?
            },
            TransactionRequirement::RevisionLock{pointer, required_revision} => {
                 check_revision(get_state(&pointer)?, required_revision)?
            },
            TransactionRequirement::VersionBump{pointer, required_revision} => {
                 check_revision(get_state(&pointer)?, required_revision)?
            },
            TransactionRequirement::RefcountUpdate{pointer, required_refcount, ..} => {
                 check_refcount(get_state(&pointer)?, required_refcount)?
            },
            TransactionRequirement::DataUpdate{pointer, required_revision, ..} => {
                 check_revision(get_state(&pointer)?, required_revision)?;
                 if ! object_updates.contains_key(&pointer.id) {
                     return Err(ReqErr::MissingObjectUpdate);
                 }
            },
            TransactionRequirement::KeyValueUpdate{pointer, required_revision, key_requirements} => {
                 check_kv_requirements(tx_id, get_state(&pointer)?, required_revision, key_requirements)?;
                 if ! object_updates.contains_key(&pointer.id) {
                     return Err(ReqErr::MissingObjectUpdate);
                 }
            },
        }
    }

    Ok(())
}

pub fn get_objects_with_errors(
    tx_id: transaction::Id,
    requirements: &Vec<TransactionRequirement>,
    objects: &HashMap<object::Id, TxStateRef>,
    object_updates: &HashMap<object::Id, ArcDataSlice>) -> HashSet<object::Id> {
    
    let mut oerrs = HashSet::new();

    let get_state = |ptr: &object::Pointer| -> result::Result<&TxStateRef, ReqErr> {
        match objects.get(&ptr.id) {
            Some(o) => {
                match o.borrow().locked_to_transaction {
                    None => Ok(o),
                    Some(id) => {
                        if id == tx_id {
                            Ok(o)
                        } else {
                            Err(ReqErr::TransactionCollision)
                        }
                    }
                }
            },
            None => Err(ReqErr::MissingObject)
        }
    };

    for r in requirements {
        let x = match r {
            TransactionRequirement::LocalTime{..} => {
                Ok(())
            },
            TransactionRequirement::RevisionLock{pointer, required_revision} => {
                match get_state(&pointer) {
                    Err(_) => Err(pointer.id),
                    Ok(obj) => {
                        check_revision(obj, required_revision).map_err(|_| pointer.id)
                    }
                }
            },
            TransactionRequirement::VersionBump{pointer, required_revision} => {
                match get_state(&pointer) {
                    Err(_) => Err(pointer.id),
                    Ok(obj) => {
                        check_revision(obj, required_revision).map_err(|_| pointer.id)
                    }
                }
            },
            TransactionRequirement::RefcountUpdate{pointer, required_refcount, ..} => {
                match get_state(&pointer) {
                    Err(_) => Err(pointer.id),
                    Ok(obj) => {
                        check_refcount(obj, required_refcount).map_err(|_| pointer.id)
                    }
                }
            },
            TransactionRequirement::DataUpdate{pointer, required_revision, ..} => {
                if ! object_updates.contains_key(&pointer.id) {
                    Err(pointer.id)
                } else {
                    match get_state(&pointer) {
                        Err(_) => Err(pointer.id),
                        Ok(obj) => {
                            check_revision(obj, required_revision).map_err(|_| pointer.id)
                        }
                    }
                }
            },
            TransactionRequirement::KeyValueUpdate{pointer, required_revision, key_requirements} => {
                if ! object_updates.contains_key(&pointer.id) {
                    Err(pointer.id)
                } else {
                    match get_state(&pointer) {
                        Err(_) => Err(pointer.id),
                        Ok(obj) => {
                            check_kv_requirements(tx_id, obj, required_revision, 
                                key_requirements).map_err(|_| pointer.id)
                        }
                    }
                }
            },
        };

        match x {
            Ok(_) => true,
            Err(obj_id) => oerrs.insert(obj_id)
        };
    }

    oerrs
}

fn check_localtime(requirement: &TimestampRequirement) -> Result {
    let now = hlc::system_clock::new().now();
    let ok = match requirement {
        TimestampRequirement::Equals(ts) => now == *ts,
        TimestampRequirement::LessThan(ts) => now < *ts,
        TimestampRequirement::GreaterThan(ts) => now > *ts,
    };

    if ok {
        Ok(())
    } else {
        Err(ReqErr::LocalTimeError)
    }
}

fn check_revision(state: &TxStateRef, required_revision: &object::Revision) -> Result {
    if state.borrow().metadata.revision == *required_revision {
        Ok(())
    } else {
        Err(ReqErr::RevisionMismatch)
    }
}

fn check_refcount(state: &TxStateRef, required_refcount: &object::Refcount) -> Result {
    if state.borrow().metadata.refcount == *required_refcount {
        Ok(())
    } else {
        Err(ReqErr::RefcountMismatch)
    }
}

fn check_kv_requirements(
    tx_id: transaction::Id,
    state: &TxStateRef,
    required_revision: &Option<object::Revision>,
    key_requirements: &Vec<KeyRequirement>) -> Result {

    let mut s = state.borrow_mut();

    if let Some(required_revision) = required_revision {
        if s.metadata.revision != * required_revision {
            return Err(ReqErr::RevisionMismatch)
        }
    }

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
                    let k = key;

                    match kv.content.get(&k) {
                        None => return Err(ReqErr::KeyExistenceError),
                        Some(s) => check_lock(s)?
                    }
                },
                KeyRequirement::MayExist{key} => {
                    let k = key;

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
                    let k = key;

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
                    match kv.content.get(key) {
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
                    match kv.content.get(key) {
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
                    match kv.content.get(key) {
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

        Ok(())
    } else {
        Err(ReqErr::ObjectTypeError)
    }
}


#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::{HashMap, HashSet};
    use std::rc::Rc;
    use std::sync;

    use super::*;
    use crate::ida::IDA;
    use crate::object;
    use crate::pool;
    use crate::store;


    #[test]
    fn collision() {
        let txid = uuid::Uuid::parse_str("01cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let revid = uuid::Uuid::parse_str("02cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let revid2 = uuid::Uuid::parse_str("06cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let poolid = uuid::Uuid::parse_str("03cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let objid = uuid::Uuid::parse_str("04cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let sp0 = store::Pointer::None{pool_index: 0};
        let sp1 = store::Pointer::None{pool_index: 1};

        let txid = transaction::Id(txid);

        let metadata = object::Metadata {
            revision: object::Revision(revid),
            refcount: object::Refcount{update_serial: 0, count: 1},
            timestamp: hlc::Timestamp::from(1)
        };

        let p = object::Pointer {
            id: object::Id(objid),
            pool_id: pool::Id(poolid),
            size: None,
            ida: IDA::Replication{ width: 3, write_threshold: 2},
            object_type: object::ObjectType::Data,
            store_pointers: vec![sp0, sp1]
        };

        let reqs = vec![TransactionRequirement::DataUpdate{
            pointer: p,
            required_revision: object::Revision(revid2),
            operation: object::DataUpdateOperation::Overwrite
        }];

        let s = store::State {
            id: object::Id(objid),
            store_pointer: store::Pointer::None{pool_index: 1},
            metadata,
            object_kind: object::Kind::Data,
            transaction_references: 0,
            locked_to_transaction: Some(transaction::Id(poolid)),
            data: sync::Arc::new(vec![]),
            max_size: None,
            kv_state: None
        };

        let txr = store::TxStateRef::new(&Rc::new(RefCell::new(s)));

        assert_eq!(txr.borrow().transaction_references, 1);

        let mut m = HashMap::new();
        m.insert(object::Id(objid), txr);

        let mut u = HashMap::new();
        u.insert(object::Id(objid), ArcDataSlice::from_vec(vec![0u8]));

        let r = check_requirements(txid, &reqs, &m, &u);

        assert_eq!(r, Err(ReqErr::TransactionCollision));
    }

    #[test]
    fn revision() {
        let txid = uuid::Uuid::parse_str("01cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let revid = uuid::Uuid::parse_str("02cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let poolid = uuid::Uuid::parse_str("03cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let objid = uuid::Uuid::parse_str("04cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let sp0 = store::Pointer::None{pool_index: 0};
        let sp1 = store::Pointer::None{pool_index: 1};

        let txid = transaction::Id(txid);

        let metadata = object::Metadata {
            revision: object::Revision(revid),
            refcount: object::Refcount{update_serial: 0, count: 1},
            timestamp: hlc::Timestamp::from(1)
        };

        let p = object::Pointer {
            id: object::Id(objid),
            pool_id: pool::Id(poolid),
            size: None,
            ida: IDA::Replication{ width: 3, write_threshold: 2},
            object_type: object::ObjectType::Data,
            store_pointers: vec![sp0, sp1]
        };

        let reqs = vec![TransactionRequirement::RevisionLock{
            pointer: p,
            required_revision: object::Revision(revid)
        }];

        let s = store::State {
            id: object::Id(objid),
            store_pointer: store::Pointer::None{pool_index: 1},
            metadata,
            object_kind: object::Kind::Data,
            transaction_references: 0,
            locked_to_transaction: None,
            data: sync::Arc::new(vec![]),
            max_size: None,
            kv_state: None
        };

        let txr = store::TxStateRef::new(&Rc::new(RefCell::new(s)));

        assert_eq!(txr.borrow().transaction_references, 1);

        let mut m = HashMap::new();
        m.insert(object::Id(objid), txr);

        let mut u = HashMap::new();
        u.insert(object::Id(objid), ArcDataSlice::from_vec(vec![0u8]));

        let r = check_requirements(txid, &reqs, &m, &u);

        assert!(r.is_ok());
    }

    #[test]
    fn revision_mismatch() {
        let txid = uuid::Uuid::parse_str("01cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let revid = uuid::Uuid::parse_str("02cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let revid2 = uuid::Uuid::parse_str("06cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let poolid = uuid::Uuid::parse_str("03cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let objid = uuid::Uuid::parse_str("04cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let sp0 = store::Pointer::None{pool_index: 0};
        let sp1 = store::Pointer::None{pool_index: 1};

        let txid = transaction::Id(txid);

        let metadata = object::Metadata {
            revision: object::Revision(revid),
            refcount: object::Refcount{update_serial: 0, count: 1},
            timestamp: hlc::Timestamp::from(1)
        };

        let p = object::Pointer {
            id: object::Id(objid),
            pool_id: pool::Id(poolid),
            size: None,
            ida: IDA::Replication{ width: 3, write_threshold: 2},
            object_type: object::ObjectType::Data,
            store_pointers: vec![sp0, sp1]
        };

        let reqs = vec![TransactionRequirement::DataUpdate{
            pointer: p,
            required_revision: object::Revision(revid2),
            operation: object::DataUpdateOperation::Overwrite
        }];

        let s = store::State {
            id: object::Id(objid),
            store_pointer: store::Pointer::None{pool_index: 1},
            metadata,
            object_kind: object::Kind::Data,
            transaction_references: 0,
            locked_to_transaction: None,
            data: sync::Arc::new(vec![]),
            max_size: None,
            kv_state: None
        };

        let txr = store::TxStateRef::new(&Rc::new(RefCell::new(s)));

        assert_eq!(txr.borrow().transaction_references, 1);

        let mut m = HashMap::new();
        m.insert(object::Id(objid), txr);

        let mut u = HashMap::new();
        u.insert(object::Id(objid), ArcDataSlice::from_vec(vec![0u8]));

        let r = check_requirements(txid, &reqs, &m, &u);

        assert_eq!(r, Err(ReqErr::RevisionMismatch));
    }

    #[test]
    fn missing_object_update() {
        let txid = uuid::Uuid::parse_str("01cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let revid = uuid::Uuid::parse_str("02cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let poolid = uuid::Uuid::parse_str("03cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let objid = uuid::Uuid::parse_str("04cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let sp0 = store::Pointer::None{pool_index: 0};
        let sp1 = store::Pointer::None{pool_index: 1};

        let txid = transaction::Id(txid);

        let metadata = object::Metadata {
            revision: object::Revision(revid),
            refcount: object::Refcount{update_serial: 0, count: 1},
            timestamp: hlc::Timestamp::from(1)
        };

        let p = object::Pointer {
            id: object::Id(objid),
            pool_id: pool::Id(poolid),
            size: None,
            ida: IDA::Replication{ width: 3, write_threshold: 2},
            object_type: object::ObjectType::Data,
            store_pointers: vec![sp0, sp1]
        };

        let reqs = vec![TransactionRequirement::DataUpdate{
            pointer: p,
            required_revision: object::Revision(revid),
            operation: object::DataUpdateOperation::Overwrite
        }];

        let s = store::State {
            id: object::Id(objid),
            store_pointer: store::Pointer::None{pool_index: 1},
            metadata,
            object_kind: object::Kind::Data,
            transaction_references: 0,
            locked_to_transaction: None,
            data: sync::Arc::new(vec![]),
            max_size: None,
            kv_state: None
        };

        let txr = store::TxStateRef::new(&Rc::new(RefCell::new(s)));

        assert_eq!(txr.borrow().transaction_references, 1);

        let mut m = HashMap::new();
        m.insert(object::Id(objid), txr);

        let u = HashMap::new();
        //u.insert(object::Id(objid), ArcDataSlice::from_vec(vec![0u8]));

        let r = check_requirements(txid, &reqs, &m, &u);

        assert_eq!(r, Err(ReqErr::MissingObjectUpdate));
    }

    #[test]
    fn refcount() {
        let txid = uuid::Uuid::parse_str("01cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let revid = uuid::Uuid::parse_str("02cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let poolid = uuid::Uuid::parse_str("03cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let objid = uuid::Uuid::parse_str("04cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let sp0 = store::Pointer::None{pool_index: 0};
        let sp1 = store::Pointer::None{pool_index: 1};

        let txid = transaction::Id(txid);

        let metadata = object::Metadata {
            revision: object::Revision(revid),
            refcount: object::Refcount{update_serial: 0, count: 1},
            timestamp: hlc::Timestamp::from(1)
        };

        let p = object::Pointer {
            id: object::Id(objid),
            pool_id: pool::Id(poolid),
            size: None,
            ida: IDA::Replication{ width: 3, write_threshold: 2},
            object_type: object::ObjectType::Data,
            store_pointers: vec![sp0, sp1]
        };

        let reqs = vec![TransactionRequirement::RefcountUpdate{
            pointer: p,
            required_refcount: object::Refcount{update_serial: 0, count: 1},
            new_refcount: object::Refcount{ update_serial: 1, count: 1 }
        }];

        let s = store::State {
            id: object::Id(objid),
            store_pointer: store::Pointer::None{pool_index: 1},
            metadata,
            object_kind: object::Kind::Data,
            transaction_references: 0,
            locked_to_transaction: None,
            data: sync::Arc::new(vec![]),
            max_size: None,
            kv_state: None
        };

        let txr = store::TxStateRef::new(&Rc::new(RefCell::new(s)));

        assert_eq!(txr.borrow().transaction_references, 1);

        let mut m = HashMap::new();
        m.insert(object::Id(objid), txr);

        let mut u = HashMap::new();
        u.insert(object::Id(objid), ArcDataSlice::from_vec(vec![0u8]));

        let r = check_requirements(txid, &reqs, &m, &u);

        assert!(r.is_ok());
    }

    #[test]
    fn refcount_mismatch() {
        let txid = uuid::Uuid::parse_str("01cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let revid = uuid::Uuid::parse_str("02cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let poolid = uuid::Uuid::parse_str("03cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let objid = uuid::Uuid::parse_str("04cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let sp0 = store::Pointer::None{pool_index: 0};
        let sp1 = store::Pointer::None{pool_index: 1};

        let txid = transaction::Id(txid);

        let metadata = object::Metadata {
            revision: object::Revision(revid),
            refcount: object::Refcount{update_serial: 0, count: 1},
            timestamp: hlc::Timestamp::from(1)
        };

        let p = object::Pointer {
            id: object::Id(objid),
            pool_id: pool::Id(poolid),
            size: None,
            ida: IDA::Replication{ width: 3, write_threshold: 2},
            object_type: object::ObjectType::Data,
            store_pointers: vec![sp0, sp1]
        };

        let reqs = vec![TransactionRequirement::RefcountUpdate{
            pointer: p,
            required_refcount: object::Refcount{update_serial: 9, count: 1},
            new_refcount: object::Refcount{ update_serial: 1, count: 1 }
        }];

        let s = store::State {
            id: object::Id(objid),
            store_pointer: store::Pointer::None{pool_index: 1},
            metadata,
            object_kind: object::Kind::Data,
            transaction_references: 0,
            locked_to_transaction: None,
            data: sync::Arc::new(vec![]),
            max_size: None,
            kv_state: None
        };

        let txr = store::TxStateRef::new(&Rc::new(RefCell::new(s)));

        assert_eq!(txr.borrow().transaction_references, 1);

        let mut m = HashMap::new();
        m.insert(object::Id(objid), txr);

        let mut u = HashMap::new();
        u.insert(object::Id(objid), ArcDataSlice::from_vec(vec![0u8]));

        let r = check_requirements(txid, &reqs, &m, &u);

        assert_eq!(r, Err(ReqErr::RefcountMismatch));
    }

    #[test]
    fn localtime_ok() {
        let txid = uuid::Uuid::parse_str("01cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let revid = uuid::Uuid::parse_str("02cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let poolid = uuid::Uuid::parse_str("03cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let objid = uuid::Uuid::parse_str("04cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let sp0 = store::Pointer::None{pool_index: 0};
        let sp1 = store::Pointer::None{pool_index: 1};

        let txid = transaction::Id(txid);

        let metadata = object::Metadata {
            revision: object::Revision(revid),
            refcount: object::Refcount{update_serial: 0, count: 1},
            timestamp: hlc::Timestamp::from(1)
        };

        let p = object::Pointer {
            id: object::Id(objid),
            pool_id: pool::Id(poolid),
            size: None,
            ida: IDA::Replication{ width: 3, write_threshold: 2},
            object_type: object::ObjectType::Data,
            store_pointers: vec![sp0, sp1]
        };

        let reqs = vec![TransactionRequirement::RevisionLock{
            pointer: p,
            required_revision: object::Revision(revid)
        }, TransactionRequirement::LocalTime {
            requirement: TimestampRequirement::GreaterThan(hlc::Timestamp::from(10)),
        }];

        let s = store::State {
            id: object::Id(objid),
            store_pointer: store::Pointer::None{pool_index: 1},
            metadata,
            object_kind: object::Kind::Data,
            transaction_references: 0,
            locked_to_transaction: None,
            data: sync::Arc::new(vec![]),
            max_size: None,
            kv_state: None
        };

        let txr = store::TxStateRef::new(&Rc::new(RefCell::new(s)));

        assert_eq!(txr.borrow().transaction_references, 1);

        let mut m = HashMap::new();
        m.insert(object::Id(objid), txr);

        let mut u = HashMap::new();
        u.insert(object::Id(objid), ArcDataSlice::from_vec(vec![0u8]));

        let r = check_requirements(txid, &reqs, &m, &u);

        assert!(r.is_ok());
    }

    #[test]
    fn localtime_err() {
        let txid = uuid::Uuid::parse_str("01cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let revid = uuid::Uuid::parse_str("02cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let poolid = uuid::Uuid::parse_str("03cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let objid = uuid::Uuid::parse_str("04cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let sp0 = store::Pointer::None{pool_index: 0};
        let sp1 = store::Pointer::None{pool_index: 1};

        let txid = transaction::Id(txid);

        let metadata = object::Metadata {
            revision: object::Revision(revid),
            refcount: object::Refcount{update_serial: 0, count: 1},
            timestamp: hlc::Timestamp::from(1)
        };

        let p = object::Pointer {
            id: object::Id(objid),
            pool_id: pool::Id(poolid),
            size: None,
            ida: IDA::Replication{ width: 3, write_threshold: 2},
            object_type: object::ObjectType::Data,
            store_pointers: vec![sp0, sp1]
        };

        let reqs = vec![TransactionRequirement::RevisionLock{
            pointer: p,
            required_revision: object::Revision(revid)
        }, TransactionRequirement::LocalTime {
            requirement: TimestampRequirement::LessThan(hlc::Timestamp::from(10)),
        }];

        let s = store::State {
            id: object::Id(objid),
            store_pointer: store::Pointer::None{pool_index: 1},
            metadata,
            object_kind: object::Kind::Data,
            transaction_references: 0,
            locked_to_transaction: None,
            data: sync::Arc::new(vec![]),
            max_size: None,
            kv_state: None
        };

        let txr = store::TxStateRef::new(&Rc::new(RefCell::new(s)));

        assert_eq!(txr.borrow().transaction_references, 1);

        let mut m = HashMap::new();
        m.insert(object::Id(objid), txr);

        let mut u = HashMap::new();
        u.insert(object::Id(objid), ArcDataSlice::from_vec(vec![0u8]));

        let r = check_requirements(txid, &reqs, &m, &u);

        assert_eq!(r, Err(ReqErr::LocalTimeError));
    }

    #[test]
    fn key_requirements() {
        let txid = uuid::Uuid::parse_str("01cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let revid = uuid::Uuid::parse_str("02cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let poolid = uuid::Uuid::parse_str("03cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let objid = uuid::Uuid::parse_str("04cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let sp0 = store::Pointer::None{pool_index: 0};
        let sp1 = store::Pointer::None{pool_index: 1};

        let txid = transaction::Id(txid);

        let metadata = object::Metadata {
            revision: object::Revision(revid),
            refcount: object::Refcount{update_serial: 0, count: 1},
            timestamp: hlc::Timestamp::from(1)
        };

        let p = object::Pointer {
            id: object::Id(objid),
            pool_id: pool::Id(poolid),
            size: None,
            ida: IDA::Replication{ width: 3, write_threshold: 2},
            object_type: object::ObjectType::Data,
            store_pointers: vec![sp0, sp1]
        };

        let k1 = [0u8, 1u8];
        let k2 = [2u8, 3u8];
        let k3 = [4u8, 5u8];
        let k4 = [6u8, 7u8];

        let mut content: HashMap<object::Key, object::KVEntry> = HashMap::new();

        content.insert(object::Key::from_bytes(&k1), object::KVEntry {
            value: object::Value::from_bytes(&k2),
            revision: object::Revision(revid),
            timestamp: hlc::Timestamp::from(2),
            locked_to_transaction: None
        });

        content.insert(object::Key::from_bytes(&k4), object::KVEntry {
            value: object::Value::from_bytes(&k2),
            revision: object::Revision(revid),
            timestamp: hlc::Timestamp::from(2),
            locked_to_transaction: Some(transaction::Id(poolid))
        });

        let mut hs = HashSet::new();

        hs.insert(object::Key::from_bytes(&k3));

        let s = store::State {
            id: object::Id(objid),
            store_pointer: store::Pointer::None{pool_index: 1},
            metadata,
            object_kind: object::Kind::Data,
            transaction_references: 0,
            locked_to_transaction: None,
            data: sync::Arc::new(vec![]),
            max_size: None,
            kv_state: Some(Box::new(object::KVObjectState{
                min: None,
                max: None,
                left: None,
                right: None,
                content,
                no_existence_locks: hs
            }))
        };

        

        let txr = store::TxStateRef::new(&Rc::new(RefCell::new(s)));

        let mut m = HashMap::new();
        m.insert(object::Id(objid), txr);

        //----------

        let reqs = vec![TransactionRequirement::KeyValueUpdate{
            pointer: p.clone(),
            required_revision: Some(object::Revision(revid)),
            key_requirements: vec![
                KeyRequirement::Exists{ key: object::Key::from_bytes(&k1) }
            ]
        }];

        let mut u = HashMap::new();
        u.insert(object::Id(objid), ArcDataSlice::from_vec(vec![0u8]));

        let r = check_requirements(txid, &reqs, &m, &u);

        assert!(r.is_ok());

        //----------

        let reqs = vec![TransactionRequirement::KeyValueUpdate{
            pointer: p.clone(),
            required_revision: None,
            key_requirements: vec![
                KeyRequirement::Exists{ key: object::Key::from_bytes(&k1) }
            ]
        }];

        let r = check_requirements(txid, &reqs, &m, &u);

        assert!(r.is_ok());

        //----------

        let reqs = vec![TransactionRequirement::KeyValueUpdate{
            pointer: p.clone(),
            required_revision: None,
            key_requirements: vec![
                KeyRequirement::MayExist{ key: object::Key::from_bytes(&k1) }
            ]
        }];

        let r = check_requirements(txid, &reqs, &m, &u);

        assert!(r.is_ok());

        //----------

        let reqs = vec![TransactionRequirement::KeyValueUpdate{
            pointer: p.clone(),
            required_revision: None,
            key_requirements: vec![
                KeyRequirement::MayExist{ key: object::Key::from_bytes(&k2) }
            ]
        }];

        let r = check_requirements(txid, &reqs, &m, &u);

        assert!(r.is_ok());

        //----------

        let reqs = vec![TransactionRequirement::KeyValueUpdate{
            pointer: p.clone(),
            required_revision: Some(object::Revision(poolid)),
            key_requirements: vec![
                KeyRequirement::Exists{ key: object::Key::from_bytes(&k1) }
            ]
        }];

        let r = check_requirements(txid, &reqs, &m, &u);

        assert_eq!(r, Err(ReqErr::RevisionMismatch));

        //----------

        let reqs = vec![TransactionRequirement::KeyValueUpdate{
            pointer: p.clone(),
            required_revision: None,
            key_requirements: vec![
                KeyRequirement::Exists{ key: object::Key::from_bytes(&k2) }
            ]
        }];

        let r = check_requirements(txid, &reqs, &m, &u);

        assert_eq!(r, Err(ReqErr::KeyExistenceError));

        //----------

        let reqs = vec![TransactionRequirement::KeyValueUpdate{
            pointer: p.clone(),
            required_revision: None,
            key_requirements: vec![
                KeyRequirement::DoesNotExist{ key: object::Key::from_bytes(&k2) }
            ]
        }];

        let r = check_requirements(txid, &reqs, &m, &u);

        assert!(r.is_ok());

        //----------

        let reqs = vec![TransactionRequirement::KeyValueUpdate{
            pointer: p.clone(),
            required_revision: None,
            key_requirements: vec![
                KeyRequirement::TimestampLessThan{ 
                    key: object::Key::from_bytes(&k1),
                    timestamp: hlc::Timestamp::from(3)
                }
            ]
        }];

        let r = check_requirements(txid, &reqs, &m, &u);

        assert!(r.is_ok());

        //----------

        let reqs = vec![TransactionRequirement::KeyValueUpdate{
            pointer: p.clone(),
            required_revision: None,
            key_requirements: vec![
                KeyRequirement::TimestampLessThan{ 
                    key: object::Key::from_bytes(&k1),
                    timestamp: hlc::Timestamp::from(1)
                }
            ]
        }];

        let r = check_requirements(txid, &reqs, &m, &u);

        assert_eq!(r, Err(ReqErr::KeyTimestampError));

        //----------

        let reqs = vec![TransactionRequirement::KeyValueUpdate{
            pointer: p.clone(),
            required_revision: None,
            key_requirements: vec![
                KeyRequirement::TimestampGreaterThan{ 
                    key: object::Key::from_bytes(&k1),
                    timestamp: hlc::Timestamp::from(1)
                }
            ]
        }];

        let r = check_requirements(txid, &reqs, &m, &u);

        assert!(r.is_ok());

        //----------

        let reqs = vec![TransactionRequirement::KeyValueUpdate{
            pointer: p.clone(),
            required_revision: None,
            key_requirements: vec![
                KeyRequirement::TimestampGreaterThan{ 
                    key: object::Key::from_bytes(&k1),
                    timestamp: hlc::Timestamp::from(3)
                }
            ]
        }];

        let r = check_requirements(txid, &reqs, &m, &u);

        assert_eq!(r, Err(ReqErr::KeyTimestampError));

        //----------

        let reqs = vec![TransactionRequirement::KeyValueUpdate{
            pointer: p.clone(),
            required_revision: None,
            key_requirements: vec![
                KeyRequirement::TimestampEquals{ 
                    key: object::Key::from_bytes(&k1),
                    timestamp: hlc::Timestamp::from(2)
                }
            ]
        }];

        let r = check_requirements(txid, &reqs, &m, &u);

        assert!(r.is_ok());

        //----------

        let reqs = vec![TransactionRequirement::KeyValueUpdate{
            pointer: p.clone(),
            required_revision: None,
            key_requirements: vec![
                KeyRequirement::TimestampEquals{ 
                    key: object::Key::from_bytes(&k1),
                    timestamp: hlc::Timestamp::from(3)
                }
            ]
        }];

        let r = check_requirements(txid, &reqs, &m, &u);

        assert_eq!(r, Err(ReqErr::KeyTimestampError));

        //---------- Existence Updates

        let reqs = vec![TransactionRequirement::KeyValueUpdate{
            pointer: p.clone(),
            required_revision: None,
            key_requirements: vec![
                KeyRequirement::Exists{ key: object::Key::from_bytes(&k4) }
            ]
        }];

        let r = check_requirements(txid, &reqs, &m, &u);

        assert_eq!(r, Err(ReqErr::TransactionCollision));

        //---------- Existence Updates

        let reqs = vec![TransactionRequirement::KeyValueUpdate{
            pointer: p.clone(),
            required_revision: None,
            key_requirements: vec![
                KeyRequirement::MayExist{ key: object::Key::from_bytes(&k3) }
            ]
        }];

        let r = check_requirements(txid, &reqs, &m, &u);

        assert_eq!(r, Err(ReqErr::TransactionCollision));
    }

    #[test]
    fn get_object_errors_none() {
        let txid = uuid::Uuid::parse_str("01cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let revid = uuid::Uuid::parse_str("02cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        //let revid2 = uuid::Uuid::parse_str("06cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let poolid = uuid::Uuid::parse_str("03cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let objid = uuid::Uuid::parse_str("04cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let objid2 = uuid::Uuid::parse_str("05cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let sp0 = store::Pointer::None{pool_index: 0};
        let sp1 = store::Pointer::None{pool_index: 1};

        let txid = transaction::Id(txid);

        let metadata = object::Metadata {
            revision: object::Revision(revid),
            refcount: object::Refcount{update_serial: 0, count: 1},
            timestamp: hlc::Timestamp::from(1)
        };

        let p = object::Pointer {
            id: object::Id(objid),
            pool_id: pool::Id(poolid),
            size: None,
            ida: IDA::Replication{ width: 3, write_threshold: 2},
            object_type: object::ObjectType::Data,
            store_pointers: vec![sp0.clone(), sp1.clone()]
        };

        let p2 = object::Pointer {
            id: object::Id(objid2),
            pool_id: pool::Id(poolid),
            size: None,
            ida: IDA::Replication{ width: 3, write_threshold: 2},
            object_type: object::ObjectType::Data,
            store_pointers: vec![sp0, sp1]
        };

        let reqs = vec![
            TransactionRequirement::DataUpdate{
                pointer: p,
                required_revision: object::Revision(revid),
                operation: object::DataUpdateOperation::Overwrite
            },
            TransactionRequirement::DataUpdate{
                pointer: p2,
                required_revision: object::Revision(revid),
                operation: object::DataUpdateOperation::Overwrite
            }
        ];

        let s = store::State {
            id: object::Id(objid),
            store_pointer: store::Pointer::None{pool_index: 1},
            metadata,
            object_kind: object::Kind::Data,
            transaction_references: 0,
            locked_to_transaction: None,
            data: sync::Arc::new(vec![]),
            max_size: None,
            kv_state: None
        };
        let s2 = store::State {
            id: object::Id(objid2),
            store_pointer: store::Pointer::None{pool_index: 1},
            metadata,
            object_kind: object::Kind::Data,
            transaction_references: 0,
            locked_to_transaction: None,
            data: sync::Arc::new(vec![]),
            max_size: None,
            kv_state: None
        };

        let txr = store::TxStateRef::new(&Rc::new(RefCell::new(s)));
        let txr2 = store::TxStateRef::new(&Rc::new(RefCell::new(s2)));

        assert_eq!(txr.borrow().transaction_references, 1);

        let mut m = HashMap::new();
        m.insert(object::Id(objid), txr);
        m.insert(object::Id(objid2), txr2);

        let mut u = HashMap::new();
        u.insert(object::Id(objid), ArcDataSlice::from_vec(vec![0u8]));
        u.insert(object::Id(objid2), ArcDataSlice::from_vec(vec![0u8]));

        let r = get_objects_with_errors(txid, &reqs, &m, &u);

        assert!(r.is_empty());
    }

    #[test]
    fn get_object_errors_one() {
        let txid = uuid::Uuid::parse_str("01cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let revid = uuid::Uuid::parse_str("02cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let revid2 = uuid::Uuid::parse_str("06cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let poolid = uuid::Uuid::parse_str("03cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let objid = uuid::Uuid::parse_str("04cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let objid2 = uuid::Uuid::parse_str("05cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let sp0 = store::Pointer::None{pool_index: 0};
        let sp1 = store::Pointer::None{pool_index: 1};

        let txid = transaction::Id(txid);

        let metadata = object::Metadata {
            revision: object::Revision(revid),
            refcount: object::Refcount{update_serial: 0, count: 1},
            timestamp: hlc::Timestamp::from(1)
        };

        let p = object::Pointer {
            id: object::Id(objid),
            pool_id: pool::Id(poolid),
            size: None,
            ida: IDA::Replication{ width: 3, write_threshold: 2},
            object_type: object::ObjectType::Data,
            store_pointers: vec![sp0.clone(), sp1.clone()]
        };

        let p2 = object::Pointer {
            id: object::Id(objid2),
            pool_id: pool::Id(poolid),
            size: None,
            ida: IDA::Replication{ width: 3, write_threshold: 2},
            object_type: object::ObjectType::Data,
            store_pointers: vec![sp0, sp1]
        };

        let reqs = vec![
            TransactionRequirement::DataUpdate{
                pointer: p,
                required_revision: object::Revision(revid),
                operation: object::DataUpdateOperation::Overwrite
            },
            TransactionRequirement::DataUpdate{
                pointer: p2,
                required_revision: object::Revision(revid2),
                operation: object::DataUpdateOperation::Overwrite
            }
        ];

        let s = store::State {
            id: object::Id(objid),
            store_pointer: store::Pointer::None{pool_index: 1},
            metadata,
            object_kind: object::Kind::Data,
            transaction_references: 0,
            locked_to_transaction: None,
            data: sync::Arc::new(vec![]),
            max_size: None,
            kv_state: None
        };
        let s2 = store::State {
            id: object::Id(objid2),
            store_pointer: store::Pointer::None{pool_index: 1},
            metadata,
            object_kind: object::Kind::Data,
            transaction_references: 0,
            locked_to_transaction: None,
            data: sync::Arc::new(vec![]),
            max_size: None,
            kv_state: None
        };

        let txr = store::TxStateRef::new(&Rc::new(RefCell::new(s)));
        let txr2 = store::TxStateRef::new(&Rc::new(RefCell::new(s2)));

        assert_eq!(txr.borrow().transaction_references, 1);

        let mut m = HashMap::new();
        m.insert(object::Id(objid), txr);
        m.insert(object::Id(objid), txr2);

        let mut u = HashMap::new();
        u.insert(object::Id(objid), ArcDataSlice::from_vec(vec![0u8]));
        u.insert(object::Id(objid2), ArcDataSlice::from_vec(vec![0u8]));

        let r = get_objects_with_errors(txid, &reqs, &m, &u);

        assert_eq!(r.len(), 1);

        let oid2 = object::Id(objid2);

        assert!(r.contains(&oid2));
    }
}
