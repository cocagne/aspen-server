use std::cell::RefCell;
use std::collections::HashMap;
use std::rc;

use aspen_server::crl;
use aspen_server::crl::null::{NullCRLState, NullCRL};
use aspen_server::data::*;
use aspen_server::hlc;
use aspen_server::ida::IDA;
use aspen_server::network::null::{NullMessenger, NullMessengerState};
use aspen_server::network::client_messages;
use aspen_server::network;
use aspen_server::object;
use aspen_server::pool;
use aspen_server::store;
use aspen_server::store::backend;
use aspen_server::store::backend::Backend;
use aspen_server::store::mock::MockStore;
use aspen_server::store::frontend::Frontend;
use aspen_server::store::simple_cache::SimpleCache;
use aspen_server::transaction;

struct TS {
    pool_id: pool::Id,
    store_id: store::Id,
    oid1: object::Id,
    oid2: object::Id,
    txid1: transaction::Id,
    txid2: transaction::Id,
    cli: network::ClientId,
    crl_state: rc::Rc<RefCell<NullCRLState>>,
    net_state: rc::Rc<RefCell<NullMessengerState>>,
    front: Frontend,
    locaters: HashMap<object::Id, store::Locater>,
    completions: rc::Rc<RefCell<Vec<backend::Completion>>>

}

struct CH {
    completions: rc::Rc<RefCell<Vec<backend::Completion>>>
}

impl backend::CompletionHandler for CH {
    fn complete(&self, op: backend::Completion) {
        self.completions.borrow_mut().push(op);
    }
}

impl TS {
    fn new() -> TS {
        let pool_id = pool::Id(uuid::Uuid::parse_str("01cccd1b-e34e-4193-ad62-868a964eab9c").unwrap());
        let oid1 = object::Id(uuid::Uuid::parse_str("02cccd1b-e34e-4193-ad62-868a964eab9c").unwrap());
        let oid2 = object::Id(uuid::Uuid::parse_str("03cccd1b-e34e-4193-ad62-868a964eab9c").unwrap());
        let txid1 = transaction::Id(uuid::Uuid::parse_str("04cccd1b-e34e-4193-ad62-868a964eab9c").unwrap());
        let txid2 = transaction::Id(uuid::Uuid::parse_str("05cccd1b-e34e-4193-ad62-868a964eab9c").unwrap());
        let cli = network::ClientId(uuid::Uuid::parse_str("06cccd1b-e34e-4193-ad62-868a964eab9c").unwrap());
        let store_id = store::Id {
            pool_uuid: pool_id.0,
            pool_index: 0
        };
        
        let completions = rc::Rc::new(RefCell::new(Vec::new()));

        let mut backend = MockStore::new(store_id);

        backend.set_completion_handler(Box::new(CH{
            completions: completions.clone()
        }));

        let backend: rc::Rc<dyn backend::Backend> = rc::Rc::new(backend);

        let object_cache = SimpleCache::new_trait_object(100);

        let (crl_state, crl) = NullCRL::new();

        let (net_state, net) = NullMessenger::new();

        let front = Frontend::new(store_id, &backend, object_cache, &crl, &net);

        TS {
            pool_id,
            store_id,
            oid1,
            oid2,
            txid1,
            txid2,
            cli,
            crl_state,
            net_state,
            front,
            locaters: HashMap::new(),
            completions: completions
        }
    }

    fn ptr(&self, oid: &object::Id, sp: &store::Pointer) -> object::Pointer {
        object::Pointer {
            id: *oid,
            pool_id: self.pool_id,
            size: None,
            ida: IDA::Replication{ width: 3, write_threshold: 2},
            store_pointers: vec![sp.clone()]
        }
    }

    fn get_ptr(&self, oid: &object::Id) -> object::Pointer {
        self.ptr(oid, &self.locaters.get(oid).unwrap().pointer)
    }

    fn alloc(&mut self, txid: transaction::Id, oid: object::Id, data: bool, content: Vec<u8>) {
        let m = client_messages::Allocate {
            to: self.store_id,
            from: self.cli,
            new_object_id: oid,
            kind: if data { object::Kind::Data } else { object::Kind::KeyValue },
            max_size: None,
            initial_refcount: object::Refcount {
                update_serial: 0,
                count: 1
            },
            data: ArcDataSlice::from_vec(content),
            timestamp: hlc::Timestamp::from(2),
            allocation_transaction_id: txid,
            revision_guard: object::AllocationRevisionGuard {
                pointer: self.ptr(&self.oid2, &store::Pointer::None {
                    pool_index: 1
                }),
                required_revision: object::Revision(self.txid2.0)
            },
        };

        assert_eq!(self.net_state.borrow().clis.len(), 0);
        assert_eq!(self.crl_state.borrow().alloc_saves, 0);

        self.front.allocate_object(m);

        assert_eq!(self.net_state.borrow().clis.len(), 0);
        assert_eq!(self.crl_state.borrow().alloc_saves, 1);

        self.front.crl_complete(crl::Completion::AllocationSave{
            store_id: self.store_id,
            transaction_id: txid,
            object_id: oid,
            success: true
        });

        assert_eq!(self.net_state.borrow().clis.len(), 1);
        assert_eq!(self.crl_state.borrow().alloc_saves, 1);

        match self.net_state.borrow_mut().clis.pop().unwrap() {
            client_messages::Message::AllocateResponse(r) => {
                self.locaters.insert(oid, store::Locater{
                    object_id: oid,
                    pointer: r.result.unwrap()
                });
            },
            _ => panic!("Bad Message")
        }
    }

    fn read(&mut self, object_id: object::Id) -> store::ReadState {
        assert_eq!(self.net_state.borrow().reads.len(), 0);
        let rid = network::RequestId(uuid::Uuid::parse_str("00cccd1b-e34e-4193-ad62-868a964eab9c").unwrap());
        self.front.read_object_for_network(self.cli, rid, self.locaters.get(&object_id).unwrap());
        //self.forward_backend_completions();
        let r = self.net_state.borrow_mut().reads.pop().unwrap();
        assert_eq!(r.client_id, self.cli);
        assert_eq!(r.request_id, rid);
        assert_eq!(r.object_id, object_id);
        r.result.unwrap()
    }

    fn forwarding_read(&mut self, object_id: object::Id) -> store::ReadState {
        assert_eq!(self.net_state.borrow().reads.len(), 0);
        let rid = network::RequestId(uuid::Uuid::parse_str("00cccd1b-e34e-4193-ad62-868a964eab9c").unwrap());
        self.front.read_object_for_network(self.cli, rid, self.locaters.get(&object_id).unwrap());
        self.forward_backend_completions();
        let r = self.net_state.borrow_mut().reads.pop().unwrap();
        assert_eq!(r.client_id, self.cli);
        assert_eq!(r.request_id, rid);
        assert_eq!(r.object_id, object_id);
        r.result.unwrap()
    }

    fn forward_backend_completions(&mut self) {
        let mut c = self.completions.borrow_mut();
        while ! c.is_empty() {
            self.front.backend_complete(c.remove(0));
        }
    }
}





#[test]
fn successful_allocation() {
    let mut ts = TS::new();

    ts.alloc(ts.txid1, ts.oid1, true, vec![0u8]);

    // Read while allocation is outstanding
    let r = ts.read(ts.oid1);

    assert_eq!(r.id, ts.oid1);
    assert_eq!(r.metadata.revision, object::Revision(ts.txid1.0));
    assert_eq!(r.metadata.refcount, object::Refcount {
        update_serial: 0,
        count: 1
    });
    assert_eq!(r.metadata.timestamp, hlc::Timestamp::from(2));
    assert_eq!(r.object_kind, object::Kind::Data);
    assert_eq!(r.data, std::sync::Arc::new(vec![0u8]));

    let r = transaction::messages::Resolved {
        to: ts.store_id,
        from: ts.store_id,
        txid: ts.txid1,
        value: true
    };

    ts.front.receive_transaction_message(transaction::messages::Message::Resolved(r));

    // Read after allocation is committed
    let r = ts.read(ts.oid1);

    assert_eq!(r.id, ts.oid1);
    assert_eq!(r.metadata.revision, object::Revision(ts.txid1.0));
    assert_eq!(r.metadata.refcount, object::Refcount {
        update_serial: 0,
        count: 1
    });
    assert_eq!(r.metadata.timestamp, hlc::Timestamp::from(2));
    assert_eq!(r.object_kind, object::Kind::Data);
    assert_eq!(r.data, std::sync::Arc::new(vec![0u8]));

    // Read after allocation is committed and written to backend
    ts.forward_backend_completions();

    let r = ts.read(ts.oid1);

    assert_eq!(r.id, ts.oid1);
    assert_eq!(r.metadata.revision, object::Revision(ts.txid1.0));
    assert_eq!(r.metadata.refcount, object::Refcount {
        update_serial: 0,
        count: 1
    });
    assert_eq!(r.metadata.timestamp, hlc::Timestamp::from(2));
    assert_eq!(r.object_kind, object::Kind::Data);
    assert_eq!(r.data, std::sync::Arc::new(vec![0u8]));

    // Read after allocation is committed and written to backend and cleared from cache
    ts.front.clear_cache();

    let r = ts.forwarding_read(ts.oid1);

    assert_eq!(r.id, ts.oid1);
    assert_eq!(r.metadata.revision, object::Revision(ts.txid1.0));
    assert_eq!(r.metadata.refcount, object::Refcount {
        update_serial: 0,
        count: 1
    });
    assert_eq!(r.metadata.timestamp, hlc::Timestamp::from(2));
    assert_eq!(r.object_kind, object::Kind::Data);
    assert_eq!(r.data, std::sync::Arc::new(vec![0u8]));
}   

#[test]
fn successful_transaction() {
    let mut ts = TS::new();

    ts.alloc(ts.txid1, ts.oid1, true, vec![0u8]);

    let r = transaction::messages::Resolved {
        to: ts.store_id,
        from: ts.store_id,
        txid: ts.txid1,
        value: true
    };

    ts.front.receive_transaction_message(transaction::messages::Message::Resolved(r));

    let r = ts.forwarding_read(ts.oid1);

    let txd = transaction::TransactionDescription {
        id: ts.txid2,
        serialized_transaction_description: ArcDataSlice::from_vec(Vec::new()),
        start_timestamp: hlc::Timestamp::from(3),
        primary_object: ts.get_ptr(&ts.oid1),
        designated_leader: 1,
        requirements: vec![transaction::requirements::TransactionRequirement::DataUpdate{
            pointer : ts.get_ptr(&ts.oid1),
            required_revision: r.metadata.revision,
            operation: object::DataUpdateOperation::Append
        }],
        finalization_actions: Vec::new(),
        originating_client: None,
        notify_on_resolution: Vec::new(),
        notes: Vec::new()
    };

    let mut object_updates = HashMap::new();

    object_updates.insert(ts.oid1, ArcDataSlice::from_vec(vec![2u8]));

    let p = transaction::messages::Prepare {
        to: ts.store_id,
        from: ts.store_id,
        proposal_id: aspen_server::paxos::ProposalId::initial_proposal_id(0),
        txd,
        object_updates,
        pre_tx_rebuilds: vec![transaction::messages::PreTransactionOpportunisticRebuild {
            object_id: ts.oid1,
            required_metadata: r.metadata,
            data: ArcDataSlice::from_vec(vec![0u8, 1u8])
        }]
    };

    ts.front.receive_transaction_message(transaction::messages::Message::Prepare(p));

    assert_eq!(ts.net_state.borrow().txs.len(), 0);

    ts.front.crl_complete(crl::Completion::TransactionSave{
        store_id: ts.store_id,
        transaction_id: ts.txid2,
        save_id: crl::TxSaveId(1),
        success: true
    });

    match ts.net_state.borrow_mut().txs.pop().unwrap() {
        transaction::Message::PrepareResponse(r) => {
            assert_eq!(r.disposition, transaction::Disposition::VoteCommit);
        },
        _ => panic!("Wrong message type")
    }

    let r = transaction::messages::Resolved {
        to: ts.store_id,
        from: ts.store_id,
        txid: ts.txid2,
        value: true
    };

    ts.front.receive_transaction_message(transaction::messages::Message::Resolved(r));

    let r = ts.forwarding_read(ts.oid1);

    assert_eq!(r.id, ts.oid1);
    assert_eq!(r.metadata.revision, object::Revision(ts.txid2.0));
    assert_eq!(r.metadata.refcount, object::Refcount {
        update_serial: 0,
        count: 1
    });
    assert_eq!(r.metadata.timestamp, hlc::Timestamp::from(3));
    assert_eq!(r.object_kind, object::Kind::Data);
    assert_eq!(r.data, std::sync::Arc::new(vec![0u8, 1u8, 2u8]));
}

