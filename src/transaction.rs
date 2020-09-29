use std::collections::HashMap;
use std::fmt;

use crate::object;
use crate::ArcDataSlice;
use crate::hlc;
use crate::network;
use crate::store;
use crate::encoding;

pub mod applyer;
pub mod checker;
pub mod locker;
pub mod messages;
pub mod requirements;
pub mod tx;

pub use requirements::{TransactionRequirement, KeyRequirement, TimestampRequirement};
pub use messages::Message;

/// Transaction UUID
/// 
/// Uniquely identifies a transaction
#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
pub struct Id(pub uuid::Uuid);

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TransactionId({})", self.0)
    }
}

/// Resolution status of a transaction
#[derive(Debug, Clone, Copy)]
pub enum Status {
    Unresolved,
    Committed,
    Aborted
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Status::Unresolved => write!(f, "Unresolved"),
            Status::Committed => write!(f, "Committed"),
            Status::Aborted => write!(f, "Aborted")
        }
    }
}

/// Disposition of the store with respect to whether or not the transaction should be committed
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Disposition {
    Undetermined,
    VoteCommit,
    VoteAbort
}

impl Disposition {
    pub fn to_u8(&self) -> u8 {
        match self {
            Disposition::Undetermined => 0u8,
            Disposition::VoteCommit => 1u8,
            Disposition::VoteAbort => 2u8
        }
    }
    pub fn from_u8(code: u8) -> Result<Disposition, crate::EncodingError> {
        match code {
            0 => Ok(Disposition::Undetermined),
            1 => Ok(Disposition::VoteCommit),
            2 => Ok(Disposition::VoteAbort),
            _ => Err(crate::EncodingError::ValueOutOfRange)
        }
    }
}

impl fmt::Display for Disposition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Disposition::Undetermined => write!(f, "Undetermined"),
            Disposition::VoteCommit => write!(f, "VoteCommit"),
            Disposition::VoteAbort => write!(f, "VoteAbort")
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ObjectUpdate {
    pub object_id: object::Id,
    pub data: ArcDataSlice
}

#[derive(Debug, Clone)]
pub struct SerializedFinalizationAction{
    pub type_uuid: uuid::Uuid,
    pub data: Vec<u8>
}

pub struct TransactionDescription {
    pub id: Id,
        serialized_transaction_description: Option<ArcDataSlice>,
    pub start_timestamp: hlc::Timestamp,
    pub primary_object: object::Pointer,
    pub designated_leader: u8,
    pub requirements: Vec<TransactionRequirement>,
    pub finalization_actions: Vec<SerializedFinalizationAction>,
    pub originating_client: Option<network::ClientId>,
    pub notify_on_resolution: Vec<store::Id>,
    pub notes: Vec<String>
}

impl TransactionDescription {

    pub fn new(id: Id, 
            start_timestamp: hlc::Timestamp,
            primary_object: object::Pointer,
            designated_leader: u8,
            requirements: Vec<TransactionRequirement>,
            finalization_actions: Vec<SerializedFinalizationAction>,
            originating_client: Option<network::ClientId>,
            notify_on_resolution: Vec<store::Id>,
            notes: Vec<String>) -> TransactionDescription {

        TransactionDescription {
            id,
            serialized_transaction_description: None,
            start_timestamp,
            primary_object,
            designated_leader,
            requirements,
            finalization_actions,
            originating_client,
            notify_on_resolution,
            notes
        }
    }

    pub fn deserialize(encoded: &ArcDataSlice) -> TransactionDescription {
        encoding::deserialize_transaction_description(encoded)
    }

    pub fn serialized_transaction_description(&mut self) -> ArcDataSlice {
        
        let ads = match &self.serialized_transaction_description {
            Some(sd) => sd.clone(),
            None => {
                let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(4096);
                let etd = encoding::encode_transaction_description(&mut builder, self);
                builder.finish(etd, None);
                let buf = builder.finished_data();
                ArcDataSlice::from_bytes(buf)
            }
        };
        let r = ads.clone();
        self.serialized_transaction_description = Some(ads);
        r
    }

    pub fn designated_leader_store_id(&self) -> store::Id {
        store::Id {
            pool_uuid: self.primary_object.pool_id.0,
            pool_index: self.designated_leader
        }
    }

    pub fn hosted_objects(&self, store_id: store::Id) -> HashMap<object::Id, store::Pointer> {
        let mut h: HashMap<object::Id, store::Pointer> = HashMap::new();

        let mut f = |ptr: &object::Pointer| {
            for sp in &ptr.store_pointers {
                if sp.pool_index() == store_id.pool_index {
                    h.insert(ptr.id, sp.clone());
                    break;
                }
            }
        };

        for r in &self.requirements {
            match r {
                TransactionRequirement::LocalTime{..}               => (),
                TransactionRequirement::RevisionLock{pointer, ..}   => f(pointer),
                TransactionRequirement::VersionBump{pointer, ..}    => f(pointer),
                TransactionRequirement::RefcountUpdate{pointer, ..} => f(pointer),
                TransactionRequirement::DataUpdate{pointer, ..}     => f(pointer),
                TransactionRequirement::KeyValueUpdate{pointer, ..} => f(pointer),
            }
        }

        h
    }
}
