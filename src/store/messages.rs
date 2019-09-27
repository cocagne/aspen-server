
use crate::network;
use crate::store;

pub enum In {
    Read {
        client_id: network::ClientId,
        request_id: network::RequestId,
        store_id: store::Id,
        locater: store::Locater
    }
}

pub enum Out {
    Read {
        client_id: network::ClientId,
        request_id: network::RequestId,
        store_id: store::Id,
        result: Result<store::ReadState, store::ReadError>
    }
}