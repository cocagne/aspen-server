use uuid;

#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
pub struct ClientId(uuid::Uuid);

#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
pub struct RequestId(uuid::Uuid);