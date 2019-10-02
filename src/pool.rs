use std::fmt;

use uuid;

/// Pool UUID
/// 
/// Uniquely identifies a storage pool
#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
pub struct Id(pub uuid::Uuid);

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PoolId({})", self.0)
    }
}
