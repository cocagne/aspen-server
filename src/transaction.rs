use std::fmt;

use crate::object;
use crate::ArcDataSlice;

/// Transaction UUID
/// 
/// Uniquely identifies a transaction
#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
pub struct Id(pub uuid::Uuid);

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
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Disposition {
    Undetermined,
    VoteCommit,
    VoteAbort
}

impl Disposition {
    pub fn to_u8(&self) -> u8 {
        match self {
            Disposition::Undetermined => 0u8,
            Disposition::VoteCommit => 1u8,
            Disposition::VoteAbort => 2u8
        }
    }
    pub fn from_u8(code: u8) -> Result<Disposition, crate::EncodingError> {
        match code {
            0 => Ok(Disposition::Undetermined),
            1 => Ok(Disposition::VoteCommit),
            2 => Ok(Disposition::VoteAbort),
            _ => Err(crate::EncodingError::ValueOutOfRange)
        }
    }
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ObjectUpdate {
    pub object_id: object::Id,
    pub data: ArcDataSlice
}
