use std::fmt;

/// Object UUID
/// 
/// Uniquely identifies an object
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct ObjectId(uuid::Uuid);

impl fmt::Display for ObjectId {
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
#[derive(Debug, Clone)]
pub struct StorePointer(Vec<u8>);

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
pub enum Type {
    Data,
    KeyValue
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Type::Data => write!(f, "Type(Data)"),
            Type::KeyValue => write!(f, "Type(KeyValue)")
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
    object_type: Type,
    id: ObjectId,
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


