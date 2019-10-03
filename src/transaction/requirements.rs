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

impl fmt::Display for TimestampRequirement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TimestampRequirement::Equals(_) => write!(f, "Equals"),
            TimestampRequirement::LessThan(_) => write!(f, "LessThan"),
            TimestampRequirement::GreaterThan(_) => write!(f, "GreaterThan")
        }
    }
}

/// Identifies a key within the serialized transaction data buffer. 
#[derive(Clone, Debug)]
pub struct Key(ArcDataSlice);

impl Key {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn test_only_from_bytes(buff: &[u8]) -> Key {
        Key(ArcDataSlice::from_bytes(buff))
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", String::from_utf8_lossy(self.0.as_bytes()))
    }
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.0.as_bytes());
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_bytes() == other.0.as_bytes()
    }
}

impl Eq for Key {}

#[derive(Clone, Debug)]
pub enum KeyRequirement {
    Exists { 
        key: Key
    },
    MayExist {
        key: Key
    },
    DoesNotExist {
        key: Key
    },
    TimestampLessThan { 
        key: Key,
        timestamp: hlc::Timestamp 
    },
    TimestampGreaterThan { 
        key: Key,
        timestamp: hlc::Timestamp 
    },
    TimestampEquals { 
        key: Key,
        timestamp: hlc::Timestamp 
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

