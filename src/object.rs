use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};

use crate::hlc;
use crate::ida;
use crate::pool;
use crate::store;
use crate::transaction;

pub mod kv_encoding;

/// Object UUID
/// 
/// Uniquely identifies an object
#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
pub struct Id(pub uuid::Uuid);

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectId({})", self.0)
    }
}

/// Object Revision
/// 
/// Revisions contain the UUID of the last transaction to successfuly update the object
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct Revision(pub uuid::Uuid);

impl Revision {
    pub fn nil() -> Revision {
        Revision(uuid::Uuid::nil())
    }
}

impl fmt::Display for Revision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Revision({})", self.0)
    }
}

/// Represents the reference count of an object
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
pub struct Refcount {

    /// Monotonically updating count of all changes made to the count. This value is used to
    /// determine which count is correct while rebuilding. Because the serial always increases
    /// the highest serial will always contain the correct count.
    pub update_serial: u32,

    /// Reference count of the object
    pub count: u32
}

impl fmt::Display for Refcount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Refcount({},{})", self.update_serial, self.count)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Kind {
    Data,
    KeyValue
}

impl Kind {
    pub fn to_u8(&self) -> u8 {
        match self {
            Kind::Data => 0u8,
            Kind::KeyValue => 1u8,
        }
    }
    pub fn from_u8(code: u8) -> Result<Kind, crate::EncodingError> {
        match code {
            0 => Ok(Kind::Data),
            1 => Ok(Kind::KeyValue),
            _ => Err(crate::EncodingError::ValueOutOfRange)
        }
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Kind::Data => write!(f, "Kind(Data)"),
            Kind::KeyValue => write!(f, "Kind(KeyValue)")
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum DataUpdateOperation {
    Overwrite,
    Append
}

impl fmt::Display for DataUpdateOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataUpdateOperation::Overwrite => write!(f, "Overwrite"),
            DataUpdateOperation::Append => write!(f, "Append")
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Metadata {
    pub revision: Revision,
    pub refcount: Refcount,
    pub timestamp: crate::hlc::Timestamp,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ObjectType {
    Data,
    KeyValue
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pointer {
    pub id: Id,
    pub pool_id: pool::Id,
    pub size: Option<u32>,
    pub ida: ida::IDA,
    pub object_type: ObjectType,
    pub store_pointers: Vec<store::Pointer>
}

impl Hash for Pointer {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Pointer {
    pub fn get_valid_acceptor_set(&self) -> HashSet<u8> {
        self.store_pointers.iter().map(|s| s.pool_index()).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AllocationRevisionGuard {
    ObjectRevisionGuard {
        pointer: Pointer,
        required_revision: Revision
    },
    KeyRevisionGuard {
        pointer: Pointer,
        key: Key,
        key_revision: Revision
    }
}

/// Identifies a key within a data buffer. Used to decouple references when
/// these are embedded in data structures
#[derive(Clone, Debug)]
pub enum Key {
    Small {
        len: u8,
        data: [u8; 23]
    },
    Large {
        data: Vec<u8>
    }
}

impl Key {
    pub fn from_bytes(bytes: &[u8]) -> Key {
        if bytes.len() <= 23 {
            let len = bytes.len() as u8;
            let mut data = [0u8; 23];
            
            for (dst, src) in data.iter_mut().zip(bytes.iter()) {
                *dst = *src
            }

            Key::Small {
                len,
                data
            }
        } else {
            let mut data: Vec<u8> = Vec::with_capacity(bytes.len());
            data.extend_from_slice(bytes);
            Key::Large {
                data
            }
        }
    }
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Key::Small{len, data} => &data[.. *len as usize],
            Key::Large{data} => &data[..]
        }
    }
    pub fn len(&self) -> usize {
       match self {
            Key::Small{len, ..} => *len as usize,
            Key::Large{data} => data.len()
        }
    }
}
    
impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", String::from_utf8_lossy(self.as_bytes()))
    }
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.as_bytes());
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for Key {}

#[derive(Clone, Debug)]
pub enum Value {
    Small {
        len: u8,
        data: [u8; 23]
    },
    Large {
        data: Vec<u8>
    }
}

impl Value {
    pub fn from_bytes(bytes: &[u8]) -> Value {
        if bytes.len() <= 23 {
            let len = bytes.len() as u8;
            let mut data = [0u8; 23];
            
            for (dst, src) in data.iter_mut().zip(bytes.iter()) {
                *dst = *src
            }
            
            Value::Small {
                len,
                data
            }
        } else {
            let mut data: Vec<u8> = Vec::with_capacity(bytes.len());
            data.extend_from_slice(bytes);
            Value::Large {
                data
            }
        }
    }
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Value::Small{len, data} => &data[.. *len as usize],
            Value::Large{data} => &data[..]
        }
    }
    pub fn len(&self) -> usize {
       match self {
            Value::Small{len, ..} => *len as usize,
            Value::Large{data} => data.len()
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for Value {}

#[derive(Debug, PartialEq, Eq)]
pub struct KVEntry {
    pub value: Value,
    pub revision: Revision,
    pub timestamp: hlc::Timestamp,
    pub locked_to_transaction: Option<transaction::Id>
}

#[derive(Debug, PartialEq, Eq)]
pub struct KVObjectState {
    pub min: Option<Key>,
    pub max: Option<Key>,
    pub left: Option<Key>,
    pub right: Option<Key>,
    pub content: HashMap<Key, KVEntry>,
    pub no_existence_locks: HashSet<Key>
}

#[derive(Debug)]
pub enum KVOpCode {
    SetMinCode      = 0, // Replicated
    SetMaxCode      = 1, // Replicated
    SetLeftCode     = 2, // IDA-encoded
    SetRightCode    = 3, // IDA-encoded
    InsertCode      = 4, // IDA-encoded value
    DeleteCode      = 5, // Not stored
    DeleteMinCode   = 6, // Not stored
    DeleteMaxCode   = 7, // Not stored
    DeleteLeftCode  = 8, // Not stored
    DeleteRightCode = 9, // Not stored
}

#[derive(Debug)]
pub struct KVOperation {
    pub operation: KVOpCode,
    pub key: Key,
    pub value: Value,
    pub timestamp: hlc::Timestamp,
    pub revision: Revision
}