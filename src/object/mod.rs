use std::fmt;
use bytes::Bytes;

/// Object UUID
/// 
/// Uniquely identifies an object
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct Id(uuid::Uuid);

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectId({})", self.0)
    }
}

/// Object Revision
/// 
/// Revisions contain the UUID of the last transaction to successfuly update the object
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct Revision(uuid::Uuid);

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

/// Optional component of an ObjectPointer that may be used to assist with locating an object
/// slice within a DataStore. For example, a flat-file store with fixed segment sizes could encode 
/// the segment offset within a StorePointer
/// 
/// This wraps a Bytes instance to take advantage of both the API the bytes crate provides as well
/// as the support for inline embedding of small data within the bytes instance rather than always
/// allocating on the heap as a Vec<u8> would.
#[derive(Debug, Clone)]
pub struct StorePointer(Bytes);

impl fmt::Display for StorePointer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.len() == 0 {
            write!(f, "StorePointer(None)")
        } else {
            write!(f, "StorePointer(len:{}, hash:{})", self.0.len(), crate::util::quick_hash(&self.0))
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Kind {
    Data,
    KeyValue
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Kind::Data => write!(f, "Kind(Data)"),
            Kind::KeyValue => write!(f, "Kind(KeyValue)")
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Crc32(u32);

impl fmt::Display for Crc32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Crc32({})", self.0)
    }
}

/// Represents the current state of an object
#[derive(Debug)]
pub struct State {
    object_kind: Kind,
    id: Id,
    revision: Revision,
    refcount: Refcount,
    timestamp: crate::hlc::Timestamp,
    store_pointer: StorePointer,
    crc: Crc32,
    segments: Vec<std::sync::Arc<Vec<u8>>>
}

impl State {
    pub fn size(&self) -> usize {
        self.segments.iter().map(|v| v.len()).sum()
    }
}

/// Public interface for object cache implementations
pub trait Cache<'a> {
  fn get(object_id: &Id) -> Option<&mut State>;

  fn put(state: State);
}

