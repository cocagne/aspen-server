
use std::fmt;

/// Uniquely identifies a data store
/// 
/// StoreIds are composed of the pool UUID to which the store belongs and the index of that
/// store within the pool
#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
pub struct Id {
    /// UUID of the storage pool this store belongs to
    pool_uuid: uuid::Uuid,

    /// Index of this store within the pool
    pool_index: u8
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StoreId({}, {})", self.pool_uuid, self.pool_index)
    }
}