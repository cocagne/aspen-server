use std::fmt;
use std::cell::RefCell;
use std::ops::Deref;
use std::rc::Rc;
use std::sync;

use crate::object;
use crate::transaction;

pub mod mock;
pub mod frontend;
pub mod backend;
pub mod manager;
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
    None {
        pool_index: u8
    },
    Short {
        pool_index: u8,
        nbytes: u8,
        content: [u8; 22]
    },
    Long {
        pool_index: u8,
        content: Vec<u8>
    }
}

impl Pointer {
    pub fn encoded_len(&self) -> usize {
        self.content_len() + 1
    }

    pub fn content_len(&self) -> usize {
        match self {
            Pointer::None{..} => 0,
            Pointer::Short{nbytes, ..} => *nbytes as usize,
            Pointer::Long{content, ..} => content.len()
        }
    }

    pub fn pool_index(&self) -> u8 {
        match self {
            Pointer::None{pool_index, ..} => *pool_index,
            Pointer::Short{pool_index, ..} => *pool_index,
            Pointer::Long{pool_index, ..} => *pool_index
        }
    }
}

impl fmt::Display for Pointer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Pointer::None{pool_index} => write!(f, "NoPointer(store:{})", pool_index),
            Pointer::Short{pool_index, nbytes, content} => write!(f, "ShortPointer(store:{}, len:{}, hash:{})", pool_index, nbytes, 
              crate::util::quick_hash(&content[0 .. *nbytes as usize])),
            Pointer::Long{pool_index, content} => write!(f, "LongPointer(store:{}, len:{}, hash:{})", pool_index, content.len(), 
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
#[derive(Debug)]
pub struct State {
    pub id: object::Id,
    pub store_pointer: Pointer,
    pub metadata: object::Metadata,
    pub object_kind: object::Kind,

    /// Used to track the number of references currently working on this object
    /// the object is not allowed to exit the cache until this number drops to 
    /// zero. The TxStateRef smart-pointer wrapper is used to increment/decrement
    /// this value.
    pub transaction_references: u32,
    pub locked_to_transaction: Option<transaction::Id>,
    pub data: sync::Arc<Vec<u8>>,
    pub crc: Crc32,
    pub max_size: Option<u32>,
    pub kv_state: Option<Box<object::KVObjectState>>,
}

impl State {
    pub fn kv_state(&mut self) -> Option<&Box<object::KVObjectState>> {
        self.kv_state.as_ref()
    }
}

/// Smart pointer for State objects that increment/decrement the state's
/// transaction_references attribute when they are crated/deleted. Each
/// transaction holding a reference to the object will do so though
/// an instance of this class
pub struct TxStateRef {
    state: Rc<RefCell<State>>
}

impl TxStateRef {
    pub fn new(state: &Rc<RefCell<State>>) -> TxStateRef {
        state.borrow_mut().transaction_references += 1;
        TxStateRef {
            state: state.clone()
        }
    }
}

impl Drop for TxStateRef {
    fn drop(&mut self) {
        self.state.borrow_mut().transaction_references -= 1;
    }
}

impl Deref for TxStateRef {
    type Target = Rc<RefCell<State>>;

    fn deref(&self) -> &Rc<RefCell<State>> {
        &self.state
    }
}



/// Public interface for object cache implementations
pub trait ObjectCache {
    fn get(&mut self, object_id: &object::Id) -> Option<&Rc<RefCell<State>>>;

    /// Inserts the given State object and optionally displaces one from
    /// the cache
    fn insert(&mut self, state: Rc<RefCell<State>>) -> Option<Rc<RefCell<State>>>;
}

#[derive(Debug, Clone)]
pub struct ReadState {
    id: object::Id,
    metadata: object::Metadata,
    object_kind: object::Kind,
    data: sync::Arc<Vec<u8>>,
}
