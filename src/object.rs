use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};

use crate::hlc;
use crate::ida;
use crate::pool;
use crate::store;


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


#[derive(Debug, Copy, Clone)]
pub struct Metadata {
    pub revision: Revision,
    pub refcount: Refcount,
    pub timestamp: crate::hlc::Timestamp,
}

pub struct Pointer {
    pub id: Id,
    pub pool_id: pool::Id,
    pub size: Option<u32>,
    pub ida: ida::IDA,
    pub store_pointers: Vec<store::Pointer>
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
            data.copy_from_slice(bytes);
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
            data.copy_from_slice(bytes);
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

#[derive(Debug)]
pub struct KeyOnlyEntry {
    pub key: Key,
    pub revision: Revision,
    pub timestamp: hlc::Timestamp
}

#[derive(Debug)]
pub struct KVEntry {
    pub value: Value,
    pub revision: Revision,
    pub timestamp: hlc::Timestamp
}

#[derive(Debug)]
pub struct KVObjectState {
    pub min: Option<KeyOnlyEntry>,
    pub max: Option<KeyOnlyEntry>,
    pub left: Option<KeyOnlyEntry>,
    pub right: Option<KeyOnlyEntry>,
    pub content: HashMap<Key, KVEntry>
}
