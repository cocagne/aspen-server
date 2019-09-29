use std::fmt;
use std::cell::RefCell;
use std::sync;

use crate::object;

pub mod mock;
pub mod frontend;
pub mod backend;
pub mod messages;
pub mod simple_cache;

#[derive(Debug, Clone, Copy)]
pub enum ReadError {
    NotFound
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ReadError::NotFound => write!(f, "NotFound"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum PutError {
    InvalidPointer
}

impl fmt::Display for PutError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PutError::InvalidPointer => write!(f, "InvalidPointer"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AllocationError {
    NoSpace
}

impl fmt::Display for AllocationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AllocationError::NoSpace => write!(f, "NoSpace"),
        }
    }
}

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

/// Pair of the object Id and optional store pointer
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Locater {
    object_id: object::Id,
    pointer: Pointer
}

#[derive(Debug, Clone, Copy)]
pub struct Crc32(pub u32);

impl fmt::Display for Crc32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Crc32({})", self.0)
    }
}

/// Represents the current state of an object
#[derive(Debug, Clone)]
pub struct State {
    id: object::Id,
    store_pointer: Pointer,
    metadata: object::Metadata,
    object_kind: object::Kind,
    transaction_locked: bool,
    data: sync::Arc<Vec<u8>>,
    crc: Crc32,
    max_size: Option<u32>
}

/// Public interface for object cache implementations
pub trait ObjectCache {
    fn get(&mut self, object_id: &object::Id) -> Option<&Box<RefCell<State>>>;

    /// Inserts the given State object and optionally displaces one from
    /// the cache
    fn insert(&mut self, state: Box<RefCell<State>>) -> Option<Box<RefCell<State>>>;
}

#[derive(Debug, Clone)]
pub struct ReadState {
    id: object::Id,
    store_pointer: Pointer,
    metadata: object::Metadata,
    object_kind: object::Kind,
    data: sync::Arc<Vec<u8>>,
    crc: Crc32
}
