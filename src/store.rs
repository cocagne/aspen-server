
use std::fmt;

/// Uniquely identifies a data store
/// 
/// StoreIds are composed of the pool UUID to which the store belongs and the index of that
/// store within the pool
#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
pub struct Id {
    /// UUID of the storage pool this store belongs to
    pub pool_uuid: uuid::Uuid,

    /// Index of this store within the pool
    pub pool_index: u8
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StoreId({}, {})", self.pool_uuid, self.pool_index)
    }
}

/// Optional component of an ObjectPointer that may be used to assist with locating an object
/// slice within a DataStore. For example, a flat-file store with fixed segment sizes could encode 
/// the segment offset within a StorePointer
/// 
/// This wraps a Bytes instance to take advantage of both the API the bytes crate provides as well
/// as the support for inline embedding of small data within the bytes instance rather than always
/// allocating on the heap as a Vec<u8> would.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Pointer {
    None,
    Short {
        nbytes: u8,
        content: [u8; 23]
    },
    Long {
        content: Vec<u8>
    }
}

impl Pointer {
    pub fn len(&self) -> usize {
        match self {
            Pointer::None => 0,
            Pointer::Short{nbytes, ..} => *nbytes as usize,
            Pointer::Long{content} => content.len()
        }
    }
}

impl fmt::Display for Pointer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Pointer::None => write!(f, "NoPointer"),
            Pointer::Short{nbytes, content} => write!(f, "ShortPointer(len:{}, hash:{})", nbytes, 
              crate::util::quick_hash(&content[0 .. *nbytes as usize])),
            Pointer::Long{content} => write!(f, "LongPointer(len:{}, hash:{})", content.len(), 
              crate::util::quick_hash(&content))
        }?;
        Ok(())
    }
}