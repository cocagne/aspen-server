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

/// Disposition of the store with respect to whether or not the transaction should be committed
#[derive(Debug, Clone, Copy)]
pub enum Disposition {
    Undetermined,
    VoteCommit,
    VoteAbort
}

impl fmt::Display for Disposition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Disposition::Undetermined => write!(f, "Undetermined"),
            Disposition::VoteCommit => write!(f, "VoteCommit"),
            Disposition::VoteAbort => write!(f, "VoteAbort")
        }
    }
}

#[derive(Clone)]
pub struct ObjectUpdate(uuid::Uuid, Bytes);

impl ObjectUpdate {
    pub fn len(&self) -> usize {
        16 + self.1.len()
    }
}