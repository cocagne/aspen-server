use std::sync;

use crossbeam::crossbeam_channel;
use tempdir::TempDir;

use aspen_server::data::*;
use aspen_server::crl;
use aspen_server::crl::RequestId;
use aspen_server::crl::sweeper;
use aspen_server::{store, object, transaction, hlc};

struct Response {
    request_id: RequestId,
    success: bool
}

struct CHandler {
    sender: crossbeam_channel::Sender<Response>
}

impl crl::RequestCompletionHandler for CHandler {
    fn transaction_save_complete(&self, request_id: RequestId, success: bool) {
        self.sender.send(Response {
            request_id,
            success
        }).unwrap();
    }
    fn allocation_save_complete(&self, request_id: RequestId, success: bool) {
        self.sender.send(Response {
            request_id,
            success
        }).unwrap();
    }
}

struct T {
    backend: Box<dyn crl::Backend>,
    crl: Box<dyn crl::Crl>,
    store_id: aspen_server::store::Id,
    receiver: crossbeam_channel::Receiver<Response>
}

impl Drop for T {
    fn drop(&mut self) {
        self.backend.shutdown();
    }
}

fn setup(tdir: &TempDir, max_file_size: usize) -> T {
    
    let backend = sweeper::recover(tdir.path(), 5, 5, 1, max_file_size).unwrap();
    let (sender, receiver) = crossbeam_channel::unbounded();
    let chandler = sync::Arc::new(CHandler {
        sender
    });
    let crl = backend.new_interface(chandler);

    let pool_uuid = uuid::Uuid::parse_str("d1cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
    let store_id = aspen_server::store::Id { pool_uuid, pool_index: 1u8 };

    T {
        backend,
        crl,
        store_id,
        receiver
    }
}

#[test]
fn initialization() {
    let tdir = TempDir::new("test").unwrap();
    let t = setup(&tdir, 10 * 1024);
    
    let (vtx, va) = t.crl.get_full_recovery_state(t.store_id);

    assert_eq!(vtx.len(), 0);
    assert_eq!(va.len(), 0);
}

#[test]
fn recovery() {
    let tdir = TempDir::new("test").unwrap();

    let mut t = setup(&tdir, 3 * 4096);

    let uu1 = uuid::Uuid::parse_str("01cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
    let uu2 = uuid::Uuid::parse_str("02cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
    let uu3 = uuid::Uuid::parse_str("03cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
    let uu4 = uuid::Uuid::parse_str("04cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();

    let txid = aspen_server::transaction::Id(uu1);
    let txd = ArcData::new(vec![0u8, 1u8, 2u8]);
    let oid1 = aspen_server::object::Id(uu2);
    let oid2 = aspen_server::object::Id(uu3);
    let ou1 = aspen_server::transaction::ObjectUpdate {
        object_id: oid1,
        data: ArcDataSlice::from_vec(vec![2u8, 3u8, 4u8])
    };
    let ou2 = aspen_server::transaction::ObjectUpdate {
        object_id: oid2,
        data: ArcDataSlice::from_vec(vec![5u8, 6u8, 7u8])
    };

    let object_updates = vec![ou1.clone(), ou2.clone()];

    let disposition = aspen_server::transaction::Disposition::VoteAbort;

    let pid1 = aspen_server::paxos::ProposalId{ number:1, peer:2 };
    let pid2 = aspen_server::paxos::ProposalId{ number:0, peer:1 };
    let acc = Some((pid2, true));
    let pax = aspen_server::paxos::PersistentState {
        promised: Some(pid1), 
        accepted: acc
    };

    let req_id = t.crl.save_transaction_state(
        t.store_id,
        txid,
        txd.clone(),
        Some(object_updates),
        disposition,
        pax
    );

    let resp = t.receiver.recv().unwrap();

    assert_eq!(resp.request_id, req_id);
    assert!(resp.success);
    
    let (vtx, va) = t.crl.get_full_recovery_state(t.store_id);

    assert_eq!(vtx.len(), 1);
    assert_eq!(va.len(), 0);

    let s = &vtx[0];

    assert_eq!(t.store_id, s.store_id);
    assert_eq!(txd, s.serialized_transaction_description);
    assert_eq!(vec![ou1.clone(), ou2.clone()], s.object_updates);
    assert_eq!(disposition, s.tx_disposition);
    assert_eq!(pax, s.paxos_state);

    //-----------

    let disposition = aspen_server::transaction::Disposition::VoteAbort;
    let pid3 = aspen_server::paxos::ProposalId{ number:2, peer:2 };
    let acc = Some((pid3, false));
    let pax = aspen_server::paxos::PersistentState {
        promised: None, 
        accepted: acc
    };

    let req_id = t.crl.save_transaction_state(
        t.store_id,
        txid,
        txd.clone(),
        None,
        disposition,
        pax
    );

    let resp = t.receiver.recv().unwrap();

    assert_eq!(resp.request_id, req_id);
    assert!(resp.success);
    
    let (vtx, va) = t.crl.get_full_recovery_state(t.store_id);

    assert_eq!(vtx.len(), 1);
    assert_eq!(va.len(), 0);

    let s = &vtx[0];

    assert_eq!(t.store_id, s.store_id);
    assert_eq!(txd, s.serialized_transaction_description);
    assert_eq!(vec![ou1.clone(), ou2.clone()], s.object_updates);
    assert_eq!(disposition, s.tx_disposition);
    assert_eq!(pax, s.paxos_state);

    // Kill CRL and restart to force load from disk
    
    drop(t);

    let mut t = setup(&tdir, 3 * 4096);

    let (vtx, va) = t.crl.get_full_recovery_state(t.store_id);

    assert_eq!(vtx.len(), 1);
    assert_eq!(va.len(), 0);

    let s = &vtx[0];

    assert_eq!(t.store_id, s.store_id);
    assert_eq!(txd, s.serialized_transaction_description);
    assert_eq!(vec![ou1.clone(), ou2.clone()], s.object_updates);
    assert_eq!(disposition, s.tx_disposition);
    assert_eq!(pax, s.paxos_state);

    // Add an Allocation, drop Tx data

    t.crl.drop_transaction_object_data(t.store_id, txid);

    let sp = store::Pointer::Short {
        nbytes: 3,
        content: [1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    };
    let kind = object::Kind::Data;
    let size = Some(5);
    let allocdata = ArcDataSlice::from_vec(vec![2u8, 3u8, 4u8]);
    let refcount = object::Refcount {
        update_serial: 10,
        count: 11
    };
    let timestamp = hlc::Timestamp::from(12);
    let atxid = transaction::Id(uu4);
    let guard = ArcDataSlice::from_vec(vec![5u8, 6u8, 7u8]);

    let req_id = t.crl.save_allocation_state(
        t.store_id,
        sp.clone(),
        oid2,
        kind,
        size,
        allocdata.clone(),
        refcount,
        timestamp,
        atxid,
        guard.clone());
    
    let resp = t.receiver.recv().unwrap();

    assert_eq!(resp.request_id, req_id);
    assert!(resp.success);

    // Kill CRL and restart to force load from disk
    
    drop(t);

    let mut t = setup(&tdir, 3 * 4096);

    let (vtx, va) = t.crl.get_full_recovery_state(t.store_id);

    assert_eq!(vtx.len(), 1);
    assert_eq!(va.len(), 1);

    let s = &vtx[0];

    assert_eq!(t.store_id, s.store_id);
    assert_eq!(txd, s.serialized_transaction_description);
    assert_eq!(s.object_updates.len(), 0); // empty due to drop tx data
    assert_eq!(disposition, s.tx_disposition);
    assert_eq!(pax, s.paxos_state);

    let a = &va[0];

    assert_eq!(t.store_id, a.store_id);
    assert_eq!(sp, a.store_pointer);
    assert_eq!(oid2, a.id);
    assert_eq!(kind, a.kind);
    assert_eq!(size, a.size);
    assert_eq!(allocdata, a.data);
    assert_eq!(refcount, a.refcount);
    assert_eq!(timestamp, a.timestamp);
    assert_eq!(atxid, a.allocation_transaction_id);
    assert_eq!(guard, a.serialized_revision_guard);

    t.crl.delete_transaction_state(t.store_id, txid);
    t.crl.delete_allocation_state(t.store_id, atxid);

    let (vtx, va) = t.crl.get_full_recovery_state(t.store_id);

    assert_eq!(vtx.len(), 0);
    assert_eq!(va.len(), 0);

    // need one more transaction save to ensure the delete states get written
    // to disk. Looks like the get_full_recovery_state can complete before
    // the write finishes. We need an acknowledged request to ensure
    // we're current
    let req_id = t.crl.save_transaction_state(
        t.store_id,
        transaction::Id(uu2),
        txd.clone(),
        None,
        disposition,
        pax
    );

    let resp = t.receiver.recv().unwrap();

    assert_eq!(resp.request_id, req_id);
    assert!(resp.success);

    // Kill CRL and restart to force load from disk
    
    drop(t);

    let t = setup(&tdir, 3 * 4096);

    let (vtx, va) = t.crl.get_full_recovery_state(t.store_id);

    assert_eq!(vtx.len(), 1);
    assert_eq!(va.len(), 0);
}