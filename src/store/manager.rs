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
use crate::object;
use crate::store;
use crate::store::backend;
use crate::store::frontend;

pub enum ReadResponse {
    Get {
        request_id: network::RequestId,
        object_id: object::Id,
        result: Result<store::ReadState, store::ReadError>
    },
}

pub enum LoadResult {
    Success(store::Id),
    Failure(store::Id, String)
}

pub trait LoadCompletionHandler {
    fn complete(&self, result: LoadResult);
}

pub enum Message {
    IOCompletion(backend::Completion),
    CRLCompletion(crl::Completion),
    RegisterReader(channel::Sender<ReadResponse>),
    Read {
        client_id: network::ClientId,
        request_id: network::RequestId,
        store_id: store::Id,
        locater: store::Locater
    },
    LoadStore {
        store_id: store::Id,
        load_fn: Box<Fn() -> Result<Box<dyn backend::Backend>, String>>,
        handler: Box<dyn LoadCompletionHandler>
    }

}

pub struct StoreManager {
    receiver: channel::Receiver<Message>,
    stores: HashMap<store::Id, frontend::Frontend>,
    readers: HashMap<network::ClientId, channel::Sender<ReadResponse>>,
    crl: Box<dyn crl::Crl>,
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
            Message::IOCompletion(completion) => {
                self.io_completion(completion)
            },
            Message::CRLCompletion(completion) => {
                self.crl_completion(completion)
            },
            Message::Read{client_id, request_id, store_id, locater} => {
                self.read(client_id, request_id, store_id, locater)
            },
            Message::RegisterReader(sender) => {
                self.register_reader(sender)
            },
            Message::LoadStore{store_id, load_fn, handler} => {
                self.load_store(store_id, load_fn, handler)
            }
        }
    }

    fn io_completion(&mut self, completion: backend::Completion) {

    }

    fn crl_completion(&mut self, completion: crl::Completion) {

    }

    fn read(
        &mut self,
        client_id: network::ClientId,
        request_id: network::RequestId,
        store_id: store::Id,
        locater: store::Locater) {

    }

    fn register_reader(&mut self, sender: channel::Sender<ReadResponse>) {

    }

    fn load_store(
        &mut self,
        store_id: store::Id, 
        load_fn: Box<Fn() -> Result<Box<dyn backend::Backend>, String>>,
        handler: Box<dyn LoadCompletionHandler>) {

        
    }
}