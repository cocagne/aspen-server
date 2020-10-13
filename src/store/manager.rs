///! Single-threaded manager for multiple data stores
///!
///! Provides channel-based linkage between the network threads, backend I/O threads and
///! the data stores they communicate with.
///!
/// 
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use crossbeam_channel;
use lru_cache;

use crate::crl;
use crate::network;
use crate::store;
use crate::store::backend;
use crate::store::frontend;
use crate::transaction;

pub enum StoreLoadResult {
    Success(store::Id),
    Failure(store::Id, String)
}

pub trait StoreLoadCompletionHandler {
    fn complete(&self, result: StoreLoadResult);
}

pub enum Message {
    IOCompletion(backend::Completion),
    CRLCompletion(crl::Completion),
    TxMessage(transaction::messages::Message),
    Read {
        client_id: network::ClientId,
        request_id: network::RequestId,
        store_id: store::Id,
        locater: store::Locater
    },
    LoadStore {
        store_id: store::Id,
        load_fn: Box<dyn Fn() -> Result<Rc<dyn backend::Backend>, String>>,
        handler: Box<dyn StoreLoadCompletionHandler>
    }

}

struct StoreState {
    pub frontend: frontend::Frontend,
    pub tx_drivers: HashMap<transaction::Id, transaction::driver::Driver>,
    pub prep_responses: lru_cache::LruCache<transaction::Id, Vec<transaction::messages::PrepareResponse>>
}

impl StoreState {
    pub fn new(frontend: frontend::Frontend) -> StoreState {
        StoreState {
            frontend,
            tx_drivers: HashMap::new(),
            prep_responses: lru_cache::LruCache::new(50)
        }
    }
}

pub struct StoreManager {
    receiver: crossbeam_channel::Receiver<Message>,
    stores: HashMap<store::Id, StoreState>,
    crl: Rc<dyn crl::Crl>,
    net: Rc<dyn network::Messenger>,
    tx_timeout: Duration,
}

impl StoreManager {
    pub fn manager_thread(&mut self) {
        let ticker = crossbeam_channel::tick(Duration::from_millis(1000));
        loop {
            crossbeam_channel::select! {
                recv(self.receiver) -> m => {
                    match m {
                        Err(_) => break,
                        Ok(msg) => self.handle_message(msg)
                    }
                }
                recv(ticker) -> _ => {
                    self.periodic_do_recovery();
                }
            }
            
        }
    }

    fn periodic_do_recovery(&mut self) {
        let now = std::time::Instant::now();

        for (_, store) in &mut self.stores {
            for (txid, tx) in &store.frontend.transactions {
                if now.duration_since(tx.get_last_event_timestamp()) > self.tx_timeout {
                    if ! store.tx_drivers.contains_key(txid) {
                        store.tx_drivers.insert(*txid, transaction::driver::Driver::recover(tx));
                    }
                }
            }

            for (_, driver) in &mut store.tx_drivers {
                driver.heartbeat();
            }
        }
    }

    fn handle_message(&mut self, msg: Message) {
        match msg  {
            Message::Read{client_id, request_id, store_id, locater} => {
                self.read(client_id, request_id, &store_id, &locater)
            },
            Message::CRLCompletion(completion) => {
                self.crl_completion(completion)
            },
            Message::IOCompletion(completion) => {
                self.io_completion(completion)
            },
            Message::LoadStore{store_id, load_fn, handler} => {
                self.load_store(store_id, load_fn, handler)
            },
            Message::TxMessage(msg) => {
                self.tx_message(msg)
            }
        }
    }

    fn io_completion(&mut self, completion: backend::Completion) {
        if let Some(store) = self.stores.get_mut(&completion.store_id()) {
            store.frontend.backend_complete(completion);
        }
    }

    fn crl_completion(&mut self, completion: crl::Completion) {
        if let Some(store) = self.stores.get_mut(&completion.store_id()) {
            store.frontend.crl_complete(completion);
        }
    }

    fn tx_message(&mut self, msg: transaction::messages::Message) {

        if let Some(store) = self.stores.get_mut(&msg.to_store()) {

            match &msg {
                transaction::messages::Message::Prepare(p) => {
                    if p.txd.designated_leader_store_id() == store.frontend.store_id {
                        match store.tx_drivers.get_mut(&msg.get_txid()) {
                            Some(_) => {},
                            None => {
                                let mut driver = transaction::driver::Driver::new(store.frontend.store_id, &self.net, p);

                                match store.prep_responses.get_mut(&p.txd.id) {
                                    Some(v) => {
                                        for pr in v {
                                            driver.receive_prepare_response(pr);
                                        }
                                        store.prep_responses.remove(&p.txd.id);
                                    },
                                    None => {}
                                }

                                store.tx_drivers.insert(msg.get_txid(), driver);
                            }
                        }
                    }
                },
                _ => {}
            }

            let driver = store.tx_drivers.get_mut(&msg.get_txid());

            match &msg {
                transaction::messages::Message::PrepareResponse(pr) => {
                    match driver {
                        Some(_) => (),
                        None => {
                            match store.prep_responses.get_mut(&pr.txid) {
                                Some(v) => v.push(pr.clone()),
                                None => {
                                    let v = vec![pr.clone()];
                                    store.prep_responses.insert(pr.txid, v);
                                }
                            }
                        }
                    }
                },
                _ => {}
            }

            if let Some(driver) = driver {
                driver.receive_transaction_message(&msg);
            };

            store.frontend.receive_transaction_message(msg);
        }
    }

    fn read(
        &mut self,
        client_id: network::ClientId,
        request_id: network::RequestId,
        store_id: &store::Id,
        locater: &store::Locater) {

        match self.stores.get_mut(store_id) {
            Some(store) => store.frontend.read_object_for_network(client_id, request_id, locater),
            None => {
                self.net.send_read_response(client_id, request_id, locater.object_id, 
                    Err(store::ReadError::StoreNotFound));
            }
        }
    }

    fn load_store(
        &mut self,
        store_id: store::Id, 
        load_fn: Box<dyn Fn() -> Result<Rc<dyn backend::Backend>, String>>,
        handler: Box<dyn StoreLoadCompletionHandler>) {

        let be = match load_fn() {
            Ok(b) => b,
            Err(err) => {
                handler.complete(StoreLoadResult::Failure(store_id, err));
                return;
            }
        };

        let backend = Rc::new(be);
        let object_cache = store::UnboundedObjectCache::new();
        let mut frontend = frontend::Frontend::new(store_id, &backend, object_cache, &self.crl, &self.net);

        let (tx_recovery, alloc_recovery) = self.crl.get_full_recovery_state(store_id);

        frontend.load_recovery_state(tx_recovery, alloc_recovery);
        
        self.stores.insert(store_id, StoreState::new(frontend));

        handler.complete(StoreLoadResult::Success(store_id));
    }
}