use std::fmt;

use crate::data::ArcDataSlice;
use crate::hlc;
use crate::object;
use std::hash::{Hash, Hasher};

#[derive(Clone, Copy, Debug)]
pub enum TimestampRequirement {
    Equals(hlc::Timestamp),
    LessThan(hlc::Timestamp),
    GreaterThan(hlc::Timestamp),
}

#[derive(Copy, Clone, Debug)]
pub enum KeyComparison {
  ByteArray,
  Integer,
  Lexical
}

impl fmt::Display for TimestampRequirement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TimestampRequirement::Equals(_) => write!(f, "Equals"),
            TimestampRequirement::LessThan(_) => write!(f, "LessThan"),
            TimestampRequirement::GreaterThan(_) => write!(f, "GreaterThan")
        }
    }
}


#[derive(Clone, Debug)]
pub enum KeyRequirement {
    Exists { 
        key: object::Key
    },
    MayExist {
        key: object::Key
    },
    DoesNotExist {
        key: object::Key
    },
    TimestampLessThan { 
        key: object::Key,
        timestamp: hlc::Timestamp 
    },
    TimestampGreaterThan { 
        key: object::Key,
        timestamp: hlc::Timestamp 
    },
    TimestampEquals { 
        key: object::Key,
        timestamp: hlc::Timestamp 
    },
    KeyRevision {
        key: object::Key,
        revision: object::Revision
    },
    KeyObjectRevision {
        key: object::Key,
        revision: object::Revision
    },
    WithinRange {
        key: object::Key,
        comparison: KeyComparison
    }
}


impl fmt::Display for KeyRequirement{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            KeyRequirement::Exists{key} => write!(f, "Exists({})", key),
            KeyRequirement::MayExist{key} => write!(f, "MayExist({})", key),
            KeyRequirement::DoesNotExist{key} => write!(f, "DoesNotEqual({})", key),
            KeyRequirement::TimestampLessThan{key, timestamp} => write!(f, "TimestampLessThan({},{})", key, timestamp),
            KeyRequirement::TimestampGreaterThan{key, timestamp} => write!(f, "TimestampGreaterThan({},{})", key, timestamp),
            KeyRequirement::TimestampEquals{key, timestamp} => write!(f, "TimestampEquals({},{})", key, timestamp)
        }
    }
}

pub enum TransactionRequirement {
    LocalTime {
      requirement: TimestampRequirement
    },

    RevisionLock {
        pointer: object::Pointer,
        required_revision: object::Revision,
    },

    VersionBump {
        pointer: object::Pointer,
        required_revision: object::Revision,
    },

    RefcountUpdate {
        pointer: object::Pointer,
        required_refcount: object::Refcount,
        new_refcount: object::Refcount
    },

    DataUpdate {
        pointer: object::Pointer,
        required_revision: object::Revision,
        operation: object::DataUpdateOperation
    },

    KeyValueUpdate {
        pointer: object::Pointer,
        required_revision: Option<object::Revision>,
        key_requirements: Vec<KeyRequirement>
    }
}

