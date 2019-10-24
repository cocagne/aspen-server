use uuid;

use crate::object;
use crate::store;
use crate::transaction;

pub mod client_messages;
pub mod null;

#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
pub struct ClientId(pub uuid::Uuid);

#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
pub struct RequestId(pub uuid::Uuid);

pub trait Messenger {
    fn send_read_response(
        &self,
        client_id: ClientId,
        request_id: RequestId,
        object_id: object::Id,
        result: Result<store::ReadState, store::ReadError>);

    fn send_transaction_message(&self, msg: transaction::Message);

    fn send_client_message(&self, msg: client_messages::Message);
}