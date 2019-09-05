use std::fmt;

/// Object UUID
/// 
/// Uniquely identifies an object
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
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
    store_pointer: super::store::Pointer,
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

