use std::fmt;

/// Object UUID
/// 
/// Uniquely identifies an object
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct ObjectId(uuid::Uuid);

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Object({})", self.0)
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
#[derive(Debug)]
pub enum StorePointer {
    None,
    Small(u64),
    Large(Vec<u8>)
}

impl fmt::Display for StorePointer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorePointer::None => write!(f, "StorePointer(None)"),
            StorePointer::Small(u) => write!(f, "StorePointer({})", u),
            StorePointer::Large(v) => write!(f, "StorePointer(len:{}, hash:{})", v.len(), crate::util::quick_hash(v)),
        }
    }
}

#[derive(Debug)]
pub enum Type {
    Data,
    KeyValue
}

#[derive(Debug)]
pub struct State {
    object_type: Type,
    id: ObjectId,
    revision: Revision,
    refcount: Refcount,
    timestamp: crate::hlc::Timestamp,
    store_pointer: StorePointer,
    segments: Vec<std::sync::Arc<Vec<u8>>>
}
