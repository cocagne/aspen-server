use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::{ArcDataSlice, DataReader, SliceReader};
use crate::hlc;
use crate::object;
use crate::object::kv_encoding;
use crate::store::TxStateRef;
use crate::transaction;

use crate::transaction::requirements::*;

use super::checker::get_objects_with_errors;

pub struct SkippedObjects(pub HashSet<object::Id>);

/// Applies transaction operations to all objects specified in the transaction IFF they
/// meet all the transaction requirements. 
/// 
/// We cannot blindly apply all transaction operations if the transaction commits because
/// it is possible for some or all objects to be in an invalid state, locked to other
/// transactions, or otherwise unfit for transaction commit. A quorum of nodes may override
/// this node and commit even when some or all objects are in an invalid state.
/// 
pub fn apply_requirements(
    tx_id: transaction::Id,
    timestamp: hlc::Timestamp,
    requirements: &Vec<TransactionRequirement>,
    objects: &HashMap<object::Id, TxStateRef>,
    object_updates: &HashMap<object::Id, ArcDataSlice>) -> SkippedObjects {

    // Get a list of all objects that are unfit for accepting Tx changes. Note that
    // any error will cause the object to show up in this list so we can safely call
    // .unwrap() for all gets in the remainder of this function.
    let oerrs = get_objects_with_errors(tx_id, requirements, objects, object_updates);

    for r in requirements {
        match r {
            TransactionRequirement::LocalTime{..} => {
                () // Nothing to apply
            },
            TransactionRequirement::RevisionLock{..} => {
                () // Nothing to apply
            },
            TransactionRequirement::VersionBump{pointer, ..} => {
                if ! oerrs.contains(&pointer.id) {
                    let state = objects.get(&pointer.id).unwrap();
                    let mut mstate = state.borrow_mut();
                    mstate.metadata.revision = object::Revision(tx_id.0);
                    mstate.metadata.timestamp = timestamp;
                }
            },
            TransactionRequirement::RefcountUpdate{pointer, new_refcount, ..} => {
                if ! oerrs.contains(&pointer.id) {
                    let state = objects.get(&pointer.id).unwrap();
                    let mut mstate = state.borrow_mut();
                    mstate.metadata.refcount = *new_refcount;
                    mstate.metadata.timestamp = timestamp;
                }
            },
            TransactionRequirement::DataUpdate{pointer, operation, ..} => {
                if ! oerrs.contains(&pointer.id) {
                    let obj = objects.get(&pointer.id).unwrap();
                    apply_data_update(tx_id, timestamp, obj, operation, 
                        object_updates.get(&pointer.id).unwrap());
                }
            },
            TransactionRequirement::KeyValueUpdate{pointer, ..} => {
                if ! oerrs.contains(&pointer.id) {
                    let obj = objects.get(&pointer.id).unwrap();
                    apply_kv_updates(tx_id, timestamp, obj, object_updates.get(&pointer.id).unwrap());
                }
            },
        }
    }

    SkippedObjects(oerrs)
}

fn apply_data_update(
    tx_id: transaction::Id,
    timestamp: hlc::Timestamp,
    state: &TxStateRef, 
    operation: &object::DataUpdateOperation,
    object_update: &ArcDataSlice) {

    let mut mstate = state.borrow_mut();
    
    mstate.metadata.revision = object::Revision(tx_id.0);
    mstate.metadata.timestamp = timestamp;

    match operation {
        object::DataUpdateOperation::Overwrite => {
            let mut v = Vec::with_capacity(object_update.len());
            v.extend_from_slice(object_update.as_bytes());
            mstate.data = Arc::new(v);
        },
        object::DataUpdateOperation::Append => {
            let mut v = Vec::with_capacity(mstate.data.len() + object_update.len());
            v.extend_from_slice(&mstate.data[..]);
            v.extend_from_slice(object_update.as_bytes());
            mstate.data = Arc::new(v);
        }
    }
}

fn apply_kv_updates(
    tx_id: transaction::Id,
    timestamp: hlc::Timestamp,
    state: &TxStateRef,
    object_update: &ArcDataSlice) {

    let mut s = state.borrow_mut();

    let kv = s.kv_state().unwrap();
        
    let data = SliceReader::new(object_update);
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

    s.data = Arc::new(kv_encoding::encode(kv));
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;
    use std::sync;

    use super::*;
    use crate::ida::IDA;
    use crate::object;
    use crate::pool;
    use crate::store;
    use crate::transaction::checker;

    #[test]
    fn overwrite_ok() {
        let txid = uuid::Uuid::parse_str("01cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let revid = uuid::Uuid::parse_str("02cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let poolid = uuid::Uuid::parse_str("03cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let objid = uuid::Uuid::parse_str("04cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let sp0 = store::Pointer::None{pool_index: 0};
        let sp1 = store::Pointer::None{pool_index: 1};

        let oid = object::Id(objid);

        let txid = transaction::Id(txid);
        let ts1 = hlc::Timestamp::from(1);
        let ts2 = hlc::Timestamp::from(2);

        let metadata = object::Metadata {
            revision: object::Revision(revid),
            refcount: object::Refcount{update_serial: 0, count: 1},
            timestamp: hlc::Timestamp::from(1)
        };

        let p = object::Pointer {
            id: oid,
            pool_id: pool::Id(poolid),
            size: None,
            ida: IDA::Replication{ width: 3, write_threshold: 2},
            store_pointers: vec![sp0, sp1]
        };

        let reqs = vec![TransactionRequirement::DataUpdate{
            pointer: p,
            required_revision: object::Revision(revid),
            operation: object::DataUpdateOperation::Overwrite
        }];

        let s = store::State {
            id: oid,
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
        m.insert(oid, txr);

        let mut u = HashMap::new();
        u.insert(oid, ArcDataSlice::from_vec(vec![0u8, 1u8, 2u8]));

        let r = checker::check_requirements(txid, &reqs, &m, &u);

        assert!(r.is_ok());

        assert_eq!(m.get(&oid).unwrap().borrow().data, Arc::new(Vec::<u8>::new()));
        assert_eq!(m.get(&oid).unwrap().borrow().metadata.timestamp, ts1);

        let a = apply_requirements(txid, ts2, &reqs, &m, &u);

        assert_eq!(a.0.len(), 0);

        assert_eq!(m.get(&oid).unwrap().borrow().data, Arc::new(vec![0u8, 1u8, 2u8]));
        assert_eq!(m.get(&oid).unwrap().borrow().metadata.timestamp, ts2);
    }

    #[test]
    fn append_ok() {
        let txid = uuid::Uuid::parse_str("01cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let revid = uuid::Uuid::parse_str("02cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let poolid = uuid::Uuid::parse_str("03cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let objid = uuid::Uuid::parse_str("04cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let sp0 = store::Pointer::None{pool_index: 0};
        let sp1 = store::Pointer::None{pool_index: 1};

        let oid = object::Id(objid);

        let txid = transaction::Id(txid);
        let ts1 = hlc::Timestamp::from(1);
        let ts2 = hlc::Timestamp::from(2);

        let metadata = object::Metadata {
            revision: object::Revision(revid),
            refcount: object::Refcount{update_serial: 0, count: 1},
            timestamp: hlc::Timestamp::from(1)
        };

        let p = object::Pointer {
            id: oid,
            pool_id: pool::Id(poolid),
            size: None,
            ida: IDA::Replication{ width: 3, write_threshold: 2},
            store_pointers: vec![sp0, sp1]
        };

        let reqs = vec![TransactionRequirement::DataUpdate{
            pointer: p,
            required_revision: object::Revision(revid),
            operation: object::DataUpdateOperation::Append
        }];

        let s = store::State {
            id: oid,
            store_pointer: store::Pointer::None{pool_index: 1},
            metadata,
            object_kind: object::Kind::Data,
            transaction_references: 0,
            locked_to_transaction: None,
            data: sync::Arc::new(vec![0u8]),
            max_size: None,
            kv_state: None
        };

        let txr = store::TxStateRef::new(&Rc::new(RefCell::new(s)));

        assert_eq!(txr.borrow().transaction_references, 1);

        let mut m = HashMap::new();
        m.insert(oid, txr);

        let mut u = HashMap::new();
        u.insert(oid, ArcDataSlice::from_vec(vec![1u8, 2u8]));

        let r = checker::check_requirements(txid, &reqs, &m, &u);

        assert!(r.is_ok());

        assert_eq!(m.get(&oid).unwrap().borrow().data, Arc::new(vec![0u8]));
        assert_eq!(m.get(&oid).unwrap().borrow().metadata.timestamp, ts1);

        let a = apply_requirements(txid, ts2, &reqs, &m, &u);

        assert_eq!(a.0.len(), 0);

        assert_eq!(m.get(&oid).unwrap().borrow().data, Arc::new(vec![0u8, 1u8, 2u8]));
        assert_eq!(m.get(&oid).unwrap().borrow().metadata.timestamp, ts2);
    }

} // end tests