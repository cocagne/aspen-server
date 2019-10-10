///! Single-threaded manager for multiple data stores
///!
///! Provides channel-based linkage between the network threads, backend I/O threads and
///! the data stores they communicate with.
///!
/// 
use std::collections::HashMap;

use crossbeam::channel;

use crate::crl;
use crate::network;
use crate::store;
use crate::store::backend;
use crate::store::frontend;

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
    Read {
        client_id: network::ClientId,
        request_id: network::RequestId,
        store_id: store::Id,
        locater: store::Locater
    },
    LoadStore {
        store_id: store::Id,
        load_fn: Box<Fn() -> Result<Box<dyn backend::Backend>, String>>,
        handler: Box<dyn StoreLoadCompletionHandler>
    }

}

pub struct StoreManager {
    receiver: channel::Receiver<Message>,
    stores: HashMap<store::Id, frontend::Frontend>,
    _crl: Box<dyn crl::Crl>,
    net: Box<dyn network::Messenger>
}

impl StoreManager {
    pub fn manager_thread(&mut self) {
        loop {
            match self.receiver.recv() {
                Err(_) => break,
                Ok(msg) => self.handle_message(msg)
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
            }
        }
    }

    fn io_completion(&mut self, completion: backend::Completion) {
        if let Some(store) = self.stores.get_mut(&completion.store_id()) {
            store.backend_complete(&self.net, completion);
        }
    }

    fn crl_completion(&mut self, _completion: crl::Completion) {

    }

    fn read(
        &mut self,
        client_id: network::ClientId,
        request_id: network::RequestId,
        store_id: &store::Id,
        locater: &store::Locater) {

        match self.stores.get_mut(store_id) {
            Some(store) => store.read(&self.net, client_id, request_id, locater),
            None => {
                self.net.send_read_response(client_id, request_id, locater.object_id, 
                    Err(store::ReadError::StoreNotFound));
            }
        }
    }

    fn load_store(
        &mut self,
        _store_id: store::Id, 
        _load_fn: Box<Fn() -> Result<Box<dyn backend::Backend>, String>>,
        _handler: Box<dyn StoreLoadCompletionHandler>) {

        
    }
}