use std::fmt;

use bytes::Bytes;

/// Transaction UUID
/// 
/// Uniquely identifies a transaction
#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
pub struct Id(uuid::Uuid);

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TransactionId({})", self.0)
    }
}

/// Resolution status of a transaction
#[derive(Debug, Clone, Copy)]
pub enum Status {
    Unresolved,
    Committed,
    Aborted
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Status::Unresolved => write!(f, "Unresolved"),
            Status::Committed => write!(f, "Committed"),
            Status::Aborted => write!(f, "Aborted")
        }
    }
}

pub struct ObjectUpdate(uuid::Uuid, Bytes);