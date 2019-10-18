use uuid;

use crate::object;
use crate::store;
use crate::transaction;

#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
pub struct ClientId(uuid::Uuid);

#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
pub struct RequestId(uuid::Uuid);

pub trait Messenger {
    fn send_read_response(
        &self,
        client_id: ClientId,
        request_id: RequestId,
        object_id: object::Id,
        result: Result<store::ReadState, store::ReadError>);

    fn send_transaction_message(&self, msg: transaction::Message);
}