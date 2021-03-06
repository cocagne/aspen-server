use std::fmt;

use crate::hlc;
use crate::object;

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

impl KeyComparison {
    pub fn compare(&self, left: &object::Key, right: &object::Key ) -> i8 {
        let l = left.as_bytes();
        let r = right.as_bytes();
        match self {
            KeyComparison::ByteArray => {
                let min = if left.len() > right.len() { left.len() } else { right.len() };

                for i in 0..min {
                    if l[i] < r[i] {
                        return -1
                    }
                    if l[i] > r[i] {
                        return 0
                    }
                }
                if l.len() < r.len() {
                    return -1
                }
                if l.len() > r.len() {
                    return 1
                }
                0
            },
            KeyComparison::Integer => {
                if l.len() == 0 && r.len() == 0 {
                    return 0
                }
                if l.len() == 0 && l.len() != 0 {
                    return -1
                }
                if l.len() != 0 && r.len() == 0 {
                    return 1
                }
            
                let big_l = num_bigint::BigInt::from_signed_bytes_be(l);
                let big_r = num_bigint::BigInt::from_signed_bytes_be(r);
                
                if big_l < big_r {
                    return -1
                }
                if big_l > big_r {
                    return 1
                }
                return 0
            },
            KeyComparison::Lexical => {
                let sl = String::from_utf8_lossy(l);
                let sr = String::from_utf8_lossy(r);

                if sl < sr {
                    return -1
                }
                if sl > sr {
                    return 1
                }
                return 0
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct KeyRevision {
  pub key: object::Key,
  pub revision: object::Revision,
}

impl fmt::Display for KeyComparison {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyComparison::ByteArray => write!(f, "KeyComparison(ByteArray)"),
            KeyComparison::Integer => write!(f, "KeyComparison(Integer)"),
            KeyComparison::Lexical => write!(f, "KeyComparison(Lexical)"),
        }
    }
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
            KeyRequirement::TimestampEquals{key, timestamp} => write!(f, "TimestampEquals({},{})", key, timestamp),
            KeyRequirement::KeyRevision{key, revision} => write!(f, "KeyRevision({},{})", key, revision),
            KeyRequirement::KeyObjectRevision{key, revision} => write!(f, "KeyObjectRevision({},{})", key, revision),
            KeyRequirement::WithinRange{key, comparison} => write!(f, "WithinRange({},{})", key, comparison),
        }
    }
}

#[derive(Clone)]
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
        full_content_lock: Vec<KeyRevision>,
        key_requirements: Vec<KeyRequirement>
    }
}

