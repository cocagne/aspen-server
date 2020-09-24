// import the flatbuffers runtime library
extern crate flatbuffers;
// import the generated code
#[allow(dead_code, unused_imports)]
#[path = "./network_protocol_generated.rs"]
mod network_protocol_generated;
pub use network_protocol_generated::com::ibm::amoeba::common::network::protocol;

use crate::transaction;
use crate::transaction::requirements;
use uuid;
use crate::data;
use crate::data::DataReader;
use crate::ida;
use crate::object;
use crate::store;
use crate::pool;
use crate::hlc;

pub fn decode_uuid(o: &protocol::UUID) -> uuid::Uuid {
    let mut d = data::DataMut::with_capacity(16);
    d.put_u64_be(o.most_sig_bits() as u64);
    d.put_u64_be(o.least_sig_bits() as u64);
    uuid::Uuid::from_slice(d.finalize().as_bytes()).unwrap()
}
pub fn encode_uuid(o: uuid::Uuid) -> protocol::UUID {
    let d = data::RawData::new(o.as_bytes());
    let msb = d.get_u64_be();
    let lsb = d.get_u64_be();
    protocol::UUID::new(msb as i64, lsb as i64)
}

pub fn decode_key_comparison(o: protocol::KeyComparison) -> requirements::KeyComparison {
    match o {
        protocol::KeyComparison::ByteArray => requirements::KeyComparison::ByteArray,
        protocol::KeyComparison::Integer => requirements::KeyComparison::Integer,
        protocol::KeyComparison::Lexical => requirements::KeyComparison::Lexical,
    }
}
pub fn encode_key_comparison(o: requirements::KeyComparison) -> protocol::KeyComparison {
    match o {
        requirements::KeyComparison::ByteArray => protocol::KeyComparison::ByteArray,
        requirements::KeyComparison::Integer => protocol::KeyComparison::Integer,
        requirements::KeyComparison::Lexical => protocol::KeyComparison::Lexical,
    }
}

pub fn decode_replication(o: &protocol::Replication) -> ida::IDA {
    ida::IDA::Replication {
        width: o.width() as u8,
        write_threshold: o.write_threshold() as u8
    }
}
pub fn decode_reed_solomon(o: &protocol::ReedSolomon) -> ida::IDA {
    ida::IDA::ReedSolomon {
        width: o.width() as u8,
        read_threshold: o.read_threshold() as u8,
        write_threshold: o.write_threshold() as u8
    }
}

pub fn decode_object_revision(o: &protocol::ObjectRevision) -> object::Revision {
    let mut d = data::DataMut::with_capacity(16);
    d.put_u64_be(o.mostSigBits() as u64);
    d.put_u64_be(o.leastSigBits() as u64);
    let u = uuid::Uuid::from_slice(d.finalize().as_bytes()).unwrap();
    object::Revision(u)
}
pub fn encode_object_revision(o: &object::Revision) -> protocol::ObjectRevision {
    let d = data::RawData::new(o.0.as_bytes());
    let msb = d.get_u64_be();
    let lsb = d.get_u64_be();
    protocol::ObjectRevision::new(msb as i64, lsb as i64)
}

pub fn decode_object_refcount(o: &protocol::ObjectRefcount) -> object::Refcount {
    object::Refcount {
        update_serial: o.update_serial() as u32,
        count: o.refcount() as u32
    }
}
pub fn encode_object_refcount(o: &object::Refcount) -> protocol::ObjectRefcount {
    protocol::ObjectRefcount::new(o.update_serial as i32, o.count as i32)
}

pub fn decode_store_pointer(o: &protocol::StorePointer) -> store::Pointer {
    let x = match o.data() {
        None => None,
        Some(s) => {
            let mut v = Vec::with_capacity(s.len());
            for &b in s {
                v.push(b as u8)
            }
            Some(&v[..])
        }
    };
    store::Pointer::new(o.store_index() as u8, x)
}
pub fn encode_store_pointer<'bldr>(builder: &'bldr mut flatbuffers::FlatBufferBuilder<'bldr>, 
    o: &store::Pointer) -> flatbuffers::WIPOffset<protocol::StorePointer<'bldr>> {
    
    fn u8toi8(a: &[u8]) -> &[i8] {
        std::mem::transmute::<&[u8], &[i8]>(a)
    }
    let (pool_index, dataOffset) = match o {
        store::Pointer::None{pool_index} => (pool_index, None),
        store::Pointer::Short{pool_index, nbytes, content, ..} => (pool_index, Some(builder.create_vector(u8toi8(&content[0..*nbytes as usize])))),
        store::Pointer::Long{pool_index, content} => (pool_index, Some(builder.create_vector(u8toi8(&content[..])))),
    };
    protocol::StorePointer::create(builder, &protocol::StorePointerArgs {
        store_index: *pool_index as i8,
        data: dataOffset
    })
}

pub fn decode_object_type(o: protocol::ObjectType) -> object::ObjectType {
    match o {
        protocol::ObjectType::Data => object::ObjectType::Data,
        protocol::ObjectType::KeyValue => object::ObjectType::KeyValue,
    }
}
pub fn encode_object_type(o: object::ObjectType) -> protocol::ObjectType {
    match o {
        object::ObjectType::Data => protocol::ObjectType::Data,
        object::ObjectType::KeyValue => protocol::ObjectType::KeyValue,
    }
}

pub fn decode_object_pointer(o: &protocol::ObjectPointer) -> object::Pointer {
    let oid = decode_uuid(o.uuid().unwrap());
    let pool_id = decode_uuid(o.pool_uuid().unwrap());
    let size = if o.size_() == 0 { None } else {Some(o.size_() as u32)};
    let object_type = decode_object_type(o.object_type());
    let ida = ida::IDA::Replication {
        width: o.ida_as_replication().unwrap().width() as u8,
        write_threshold: o.ida_as_replication().unwrap().write_threshold() as u8
    };
    let mut pointers:Vec<store::Pointer> = Vec::new();
    if let Some(v) = o.store_pointers() {
        for p in v {
            pointers.push(decode_store_pointer(&p));
        }
    }
    object::Pointer {
        id: object::Id(oid),
        pool_id: pool::Id(pool_id),
        size: size,
        ida: ida,
        object_type: object_type,
        store_pointers: pointers
    }
}
pub fn encode_object_pointer<'bldr>(builder: &'bldr mut flatbuffers::FlatBufferBuilder<'bldr>, 
    o: &object::Pointer) -> flatbuffers::WIPOffset<protocol::ObjectPointer<'bldr>> {

    let size = if let Some(sz) = o.size { sz } else { 0 };

    let mut pointer_offsets = Vec::new();

    for p in o.store_pointers {
        pointer_offsets.push(encode_store_pointer(builder, &p));
    }

    let pointers = builder.create_vector(&pointer_offsets[..]);

    let (ida_type, ida_offset) = match o.ida {
        ida::IDA::Replication{width, write_threshold} => {
            let offset = protocol::Replication::create(builder, &protocol::ReplicationArgs{
                width: width as i32,
                write_threshold: write_threshold as i32
            });
            (protocol::IDA::Replication, offset.as_union_value())
        }
        ida::IDA::ReedSolomon{width, read_threshold, write_threshold} => {
            let offset = protocol::ReedSolomon::create(builder, &protocol::ReedSolomonArgs{
                width: width as i32,
                read_threshold: read_threshold as i32,
                write_threshold: write_threshold as i32
            });
            (protocol::IDA::ReedSolomon, offset.as_union_value())
        }
    };

    protocol::ObjectPointer::create(builder, &protocol::ObjectPointerArgs {
        uuid: Some(&encode_uuid(o.id.0)),
        pool_uuid: Some(&encode_uuid(o.pool_id.0)),
        size_: size as i32,
        store_pointers: Some(pointers),
        ida_type: ida_type,
        ida: Some(ida_offset),
        object_type: encode_object_type(o.object_type)
    })
}

pub fn decode_transaction_status(o: protocol::TransactionStatus) -> transaction::Status {
    match o {
        protocol::TransactionStatus::Unresolved => transaction::Status::Unresolved,
        protocol::TransactionStatus::Aborted => transaction::Status::Aborted,
        protocol::TransactionStatus::Committed => transaction::Status::Committed
    }
}
pub fn encode_transaction_status(o: transaction::Status) -> protocol::TransactionStatus {
    match o {
        transaction::Status::Unresolved => protocol::TransactionStatus::Unresolved,
        transaction::Status::Aborted => protocol::TransactionStatus::Aborted,
        transaction::Status::Committed => protocol::TransactionStatus::Committed
    }
}

pub fn decode_transaction_disposition(o: protocol::TransactionDisposition) -> transaction::Disposition {
    match o {
        protocol::TransactionDisposition::Undetermined => transaction::Disposition::Undetermined,
        protocol::TransactionDisposition::VoteAbort => transaction::Disposition::VoteAbort,
        protocol::TransactionDisposition::VoteCommit => transaction::Disposition::VoteCommit
    }
}
pub fn encode_transaction_distposition(o: transaction::Disposition) -> protocol::TransactionDisposition {
    match o {
        transaction::Disposition::Undetermined => protocol::TransactionDisposition::Undetermined,
        transaction::Disposition::VoteAbort => protocol::TransactionDisposition::VoteAbort,
        transaction::Disposition::VoteCommit => protocol::TransactionDisposition::VoteCommit
    }
}

pub fn decode_data_update_operation(o: protocol::DataUpdateOperation) -> object::DataUpdateOperation {
    match o {
        protocol::DataUpdateOperation::Append => object::DataUpdateOperation::Append,
        protocol::DataUpdateOperation::Overwrite => object::DataUpdateOperation::Overwrite,
    }
}
pub fn encode_data_update_operation(o: object::DataUpdateOperation) -> protocol::DataUpdateOperation {
    match o {
        object::DataUpdateOperation::Append => protocol::DataUpdateOperation::Append,
        object::DataUpdateOperation::Overwrite => protocol::DataUpdateOperation::Overwrite,
    }
}

pub fn decode_key_requirement(o: &protocol::KVReq) -> requirements::KeyRequirement {
    let key = object::Key::from_bytes(std::mem::transmute::<&[i8], &[u8]>(o.key().unwrap()));
    let timestamp = hlc::Timestamp::from(o.timestamp() as u64);
    match o.requirement() {
        protocol::KeyRequirement::Exists => requirements::KeyRequirement::Exists { key },
        protocol::KeyRequirement::MayExist => requirements::KeyRequirement::MayExist { key },
        protocol::KeyRequirement::DoesNotExist => requirements::KeyRequirement::DoesNotExist { key },
        protocol::KeyRequirement::TimestampLessThan => requirements::KeyRequirement::TimestampLessThan { key, timestamp },
        protocol::KeyRequirement::TimestampGreaterThan => requirements::KeyRequirement::TimestampGreaterThan { key, timestamp },
        protocol::KeyRequirement::TimestampEquals => requirements::KeyRequirement::TimestampEquals { key, timestamp },
        protocol::KeyRequirement::KeyRevision => {
            requirements::KeyRequirement::KeyRevision { 
                key,
                revision: decode_object_revision(o.revision().unwrap())
            }
        },
        protocol::KeyRequirement::KeyObjectRevision => {
            requirements::KeyRequirement::KeyObjectRevision { 
                key,
                revision: decode_object_revision(o.revision().unwrap())
            }
        }
        protocol::KeyRequirement::WithinRange => {
            requirements::KeyRequirement::WithinRange {
                key,
                comparison: decode_key_comparison(o.comparison())
            }
        }
    }
} 
pub fn encode_key_requirement<'bldr>(builder: &'bldr mut flatbuffers::FlatBufferBuilder<'bldr>, 
    o: &requirements::KeyRequirement) -> flatbuffers::WIPOffset<protocol::KVReq<'bldr>> {

    let args = match o {
        requirements::KeyRequirement::Exists{ key } => {
            protocol::KVReqArgs {
                requirement: protocol::KeyRequirement::Exists,
                timestamp: 0,
                revision: None,
                comparison: protocol::KeyComparison::ByteArray,
                key: Some(builder.create_vector(std::mem::transmute::<&[u8],&[i8]>(key.as_bytes()))),
            }
        },
        requirements::KeyRequirement::MayExist{ key } => {
            protocol::KVReqArgs {
                requirement: protocol::KeyRequirement::MayExist,
                timestamp: 0,
                revision: None,
                comparison: protocol::KeyComparison::ByteArray,
                key: Some(builder.create_vector(std::mem::transmute::<&[u8],&[i8]>(key.as_bytes()))),
            }
        },
        requirements::KeyRequirement::DoesNotExist{ key } => {
            protocol::KVReqArgs {
                requirement: protocol::KeyRequirement::DoesNotExist,
                timestamp: 0,
                revision: None,
                comparison: protocol::KeyComparison::ByteArray,
                key: Some(builder.create_vector(std::mem::transmute::<&[u8],&[i8]>(key.as_bytes()))),
            }
        },
        requirements::KeyRequirement::TimestampLessThan{ key, timestamp } => {
            protocol::KVReqArgs {
                requirement: protocol::KeyRequirement::TimestampLessThan,
                timestamp: timestamp.to_u64() as i64,
                revision: None,
                comparison: protocol::KeyComparison::ByteArray,
                key: Some(builder.create_vector(std::mem::transmute::<&[u8],&[i8]>(key.as_bytes()))),
            }
        },
        requirements::KeyRequirement::TimestampGreaterThan{ key, timestamp } => {
            protocol::KVReqArgs {
                requirement: protocol::KeyRequirement::TimestampGreaterThan,
                timestamp: timestamp.to_u64() as i64,
                revision: None,
                comparison: protocol::KeyComparison::ByteArray,
                key: Some(builder.create_vector(std::mem::transmute::<&[u8],&[i8]>(key.as_bytes()))),
            }
        },
        requirements::KeyRequirement::TimestampEquals{ key, timestamp } => {
            protocol::KVReqArgs {
                requirement: protocol::KeyRequirement::TimestampEquals,
                timestamp: timestamp.to_u64() as i64,
                revision: None,
                comparison: protocol::KeyComparison::ByteArray,
                key: Some(builder.create_vector(std::mem::transmute::<&[u8],&[i8]>(key.as_bytes()))),
            }
        },
        requirements::KeyRequirement::KeyRevision{ key, revision } => {
            protocol::KVReqArgs {
                requirement: protocol::KeyRequirement::KeyRevision,
                timestamp: 0,
                revision: Some(&encode_object_revision(revision)),
                comparison: protocol::KeyComparison::ByteArray,
                key: Some(builder.create_vector(std::mem::transmute::<&[u8],&[i8]>(key.as_bytes()))),
            }
        },
        requirements::KeyRequirement::WithinRange{ key, comparison } => {
            protocol::KVReqArgs {
                requirement: protocol::KeyRequirement::WithinRange,
                timestamp: 0,
                revision: None,
                comparison: encode_key_comparison(*comparison),
                key: Some(builder.create_vector(std::mem::transmute::<&[u8],&[i8]>(key.as_bytes()))),
            }
        },
    };
    protocol::KVReq::create(builder, &args)
}



pub fn decode_data_update(o: &protocol::DataUpdate) -> requirements::TransactionRequirement {
    let pointer = decode_object_pointer(&o.object_pointer().unwrap());
    let required_revision = decode_object_revision(o.required_revision().unwrap());
    let operation = decode_data_update_operation(o.operation());
    requirements::TransactionRequirement::DataUpdate {
        pointer,
        required_revision,
        operation
    }
}

pub fn decode_refcount_udpate(o: &protocol::RefcountUpdate) -> requirements::TransactionRequirement {
    let pointer = decode_object_pointer(&o.object_pointer().unwrap());
    let required_refcount = decode_object_refcount(o.required_refcount().unwrap());
    let new_refcount = decode_object_refcount(o.new_refcount().unwrap());
    requirements::TransactionRequirement::RefcountUpdate {
        pointer,
        required_refcount,
        new_refcount
    }
}

pub fn decode_version_bump(o: &protocol::VersionBump) -> requirements::TransactionRequirement {
    let pointer = decode_object_pointer(&o.object_pointer().unwrap());
    let required_revision = decode_object_revision(o.required_revision().unwrap());
    
    requirements::TransactionRequirement::VersionBump {
        pointer,
        required_revision,
    }
}

pub fn decode_revision_lock(o: &protocol::RevisionLock) -> requirements::TransactionRequirement {
    let pointer = decode_object_pointer(&o.object_pointer().unwrap());
    let required_revision = decode_object_revision(o.required_revision().unwrap());
    
    requirements::TransactionRequirement::RevisionLock {
        pointer,
        required_revision,
    }
}

pub fn decode_serialized_finalization_action(o: &protocol::SerializedFinalizationAction) -> transaction::SerializedFinalizationAction {
    let tid = decode_uuid(o.type_uuid().unwrap());
    let slice = o.data().unwrap();
    let v:Vec<u8> = Vec::with_capacity(slice.len());
    v.extend_from_slice(std::mem::transmute::<&[i8],&[u8]>(slice));

    transaction::SerializedFinalizationAction {
        type_uuid: tid,
        data: v
    }
}
pub fn encode_serialized_finalization_action<'bldr>(builder: &'bldr mut flatbuffers::FlatBufferBuilder<'bldr>, 
    o: &transaction::SerializedFinalizationAction) -> flatbuffers::WIPOffset<protocol::SerializedFinalizationAction<'bldr>> {

    protocol::SerializedFinalizationAction::create(builder, &protocol::SerializedFinalizationActionArgs {
        type_uuid: Some(&encode_uuid(o.type_uuid)),
        data: Some(builder.create_vector(std::mem::transmute::<&[u8],&[i8]>(&o.data[..])))
    })
}

pub fn decode_timestamp_requirement(o: protocol::LocalTimeRequirementEnum, timestamp:i64) -> requirements::TimestampRequirement {
    match o {
        protocol::LocalTimeRequirementEnum::GreaterThan => requirements::TimestampRequirement::GreaterThan(hlc::Timestamp::from(timestamp as u64)),
        protocol::LocalTimeRequirementEnum::LessThan => requirements::TimestampRequirement::LessThan(hlc::Timestamp::from(timestamp as u64))
    }
}
pub fn encode_time_requirement(o: requirements::TimestampRequirement) -> protocol::LocalTimeRequirementEnum {
    match o {
        requirements::TimestampRequirement::Equals(_) => protocol::LocalTimeRequirementEnum::GreaterThan,
        requirements::TimestampRequirement::GreaterThan(_) => protocol::LocalTimeRequirementEnum::GreaterThan,
        requirements::TimestampRequirement::LessThan(_) => protocol::LocalTimeRequirementEnum::LessThan,
    }
}

pub fn decode_local_time_requirement(o: &protocol::LocalTimeRequirement) -> requirements::TransactionRequirement {
    let tr = decode_timestamp_requirement(o.requirement(), o.timestamp());
    requirements::TransactionRequirement::LocalTime {
        requirement: tr
    }
}
pub fn encode_local_time_requirement<'bldr>(builder: &'bldr mut flatbuffers::FlatBufferBuilder<'bldr>, 
o: &requirements::TimestampRequirement) -> flatbuffers::WIPOffset<protocol::LocalTimeRequirement<'bldr>> {
    let ts = match o {
        requirements::TimestampRequirement::Equals(ts) => ts.to_u64() as i64,
        requirements::TimestampRequirement::GreaterThan(ts) => ts.to_u64() as i64,
        requirements::TimestampRequirement::LessThan(ts) => ts.to_u64() as i64
    };
    protocol::LocalTimeRequirement::create(builder, &protocol::LocalTimeRequirementArgs {
        timestamp: ts,
        requirement: encode_time_requirement(*o)
    })
}

pub fn decode_store_id(o: &protocol::StoreId) -> store::Id {
    let pool = decode_uuid(o.storage_pool_uuid().unwrap());
    let index = o.storage_pool_index();
    
    store::Id {
        pool_uuid: pool,
        pool_index: index as u8
    }
}
pub fn encode_store_id<'bldr>(builder: &'bldr mut flatbuffers::FlatBufferBuilder<'bldr>, 
    o: &store::Id) -> flatbuffers::WIPOffset<protocol::StoreId<'bldr>> {

    protocol::StoreId::create(builder, &protocol::StoreIdArgs {
        storage_pool_uuid: Some(&encode_uuid(o.pool_uuid)),
        storage_pool_index: o.pool_index as i8
    })
}

pub fn decode_transaction_requirement(o: &protocol::TransactionRequirement) -> transaction::TransactionRequirement {
    if let Some(r) = o.data_update() {
        return decode_data_update(&r)
    };
    if let Some(r) = o.refcount_update() {
        return decode_refcount_udpate(&r)
    };
    if let Some(r) = o.version_bump() {
        return decode_version_bump(&r)
    };
    if let Some(r) = o.revision_lock() {
        return decode_revision_lock(&r)
    };
    if let Some(r) = o.kv_update() {
        let object_pointer = decode_object_pointer(&r.object_pointer().unwrap());
        let required_revision = decode_object_revision(&r.required_revision().unwrap());
        let full_content_lock: Vec<requirements::KeyRevision> = Vec::new();
        if let Some(v) = r.content_lock() {
            for kr in v {
                full_content_lock.push(requirements::KeyRevision {
                    key: object::Key::from_bytes(std::mem::transmute::<&[i8], &[u8]>(kr.key().unwrap())),
                    revision: decode_object_revision(&kr.revision().unwrap())
                });
            }
        };
        let kv_reqs: Vec<requirements::KeyRequirement> = Vec::new();
        if let Some(v) = r.requirements() {
            for r in v {
                kv_reqs.push(decode_key_requirement(&r));
            }
        };
    };

    return decode_local_time_requirement(&o.localtime().unwrap())
}
pub fn encode_transaction_requirement<'bldr>(builder: &'bldr mut flatbuffers::FlatBufferBuilder<'bldr>, 
    o: &requirements::TimestampRequirement) -> flatbuffers::WIPOffset<protocol::TransactionRequirement<'bldr>> {

    ()
}

pub fn decode_transaction_description(o: &protocol::TransactionDescription) -> transaction::TransactionDescription {
    let txid = transaction::Id(decode_uuid(o.transaction_uuid().unwrap()));
    let start_ts = hlc::Timestamp::from(o.start_timestamp() as u64);
    let primary_object = decode_object_pointer(&o.primary_object().unwrap());
    let designated_leader = o.designated_leader_uid();
    let originating_client = decode_uuid(o.originating_client().unwrap());

    let mut notify_on_resolution:Vec<store::Id> = Vec::new();
    if let Some(v) = o.notify_on_resolution() {
        for p in v {
            notify_on_resolution.push(decode_store_id(&p));
        }
    };

    let mut finalization_actions:Vec<transaction::SerializedFinalizationAction> = Vec::new();
    if let Some(v) = o.finalization_actions() {
        for p in v {
            finalization_actions.push(decode_serialized_finalization_action(&p));
        }
    };

    let notes = String::from_utf8_lossy(std::mem::transmute::<&[i8],&[u8]>(o.notes().unwrap()));

    let transaction_requirements: Vec<requirements::TransactionRequirement> = Vec::new();
    if let Some(v) = o.requirements() {
        for p in v {

        }
    }
    ()
}