use std::collections::{HashMap, HashSet};

use integer_encoding::*;

use crate::hlc;
use crate::object;
use crate::{DataMut, RawData, DataReader};

#[derive(Debug)]
pub struct CorruptedObject;

///! Encoded format
///! <1-byte mask for min/max/left/right presence>[min][max][left][right][kv-pair1][kv-pair2]...
///! Where min/max/left/right are
///!   <varint-key-len><key>
///! kv-pairs are
///!   <varint-key-len><key><16-byte-revision><8-byte-timestamp><varint-value-len><value>

pub fn encoded_size(state: &object::KVObjectState) -> usize {
    let klen = |key: &object::Key| -> usize {
        let len = key.len();
        len.required_space() + len
    };
    let vlen = |value: &object::Value| -> usize {
        let len = value.len();
        16 + 8 + len.required_space() + len
    };

    let mut sz = 1;

    if let Some(k) = &state.min {
        sz += klen(k);
    }
    if let Some(k) = &state.min {
        sz += klen(k);
    }
    if let Some(k) = &state.left {
        sz += klen(k);
    }
    if let Some(k) = &state.right {
        sz += klen(k);
    }
    for (k,v) in &state.content {
        sz += klen(k);
        sz += vlen(&v.value);
    }

    sz
}

pub fn encode(state: &object::KVObjectState) -> Vec<u8> {
    let mut d = DataMut::with_capacity(encoded_size(state));

    let mut mask:u8 = 0;

    if state.min.is_some()   { mask |= 1 << 3 }
    if state.max.is_some()   { mask |= 1 << 2 }
    if state.left.is_some()  { mask |= 1 << 1 }
    if state.right.is_some() { mask |= 1 << 0 }

    d.put_u8(mask);

    let put_key = |d: &mut DataMut, key: &object::Key| {
        let mut buf = [0u8;16];
        let nbytes = key.len().encode_var(&mut buf[..]);
        d.put_slice(&buf[..nbytes]);
        d.put_slice(key.as_bytes());
    };

    let put_entry = |d: &mut DataMut, e: &object::KVEntry| {
        d.put_uuid(e.revision.0);
        d.put_u64_le(e.timestamp.into());

        let mut buf = [0u8;16];
        let nbytes = e.value.len().encode_var(&mut buf[..]);
        d.put_slice(&buf[..nbytes]);
        d.put_slice(e.value.as_bytes());
    };

    if let Some(k) = &state.min {
        put_key(&mut d, k);
    }
    if let Some(k) = &state.max {
        put_key(&mut d, k);
    }
    if let Some(k) = &state.left {
        put_key(&mut d, k);
    }
    if let Some(k) = &state.right {
        put_key(&mut d, k);
    }
    for (k,v) in &state.content {
        put_key(&mut d, k);
        put_entry(&mut d, v);
    }

    d.finalize().buffer
}

pub fn decode(buf: &[u8]) -> Result<object::KVObjectState, CorruptedObject> {

    let get_key = |b: &mut RawData| -> Result<object::Key, CorruptedObject> {
        let (key_len, varint_sz) = usize::decode_var(b.remaining_bytes());
        if b.remaining() < key_len + varint_sz {
            return Err(CorruptedObject);
        }
        b.incr_offset(varint_sz);
        let key = object::Key::from_bytes(b.get_slice(key_len));
        Ok(key)
    };

    let get_entry = |b: &mut RawData| -> Result<object::KVEntry, CorruptedObject> {
        if b.remaining() < 16 + 8 {
            return Err(CorruptedObject);
        }
        let u = b.get_uuid();
        let ts = b.get_u64_le();

        let (val_len, varint_sz) = usize::decode_var(b.remaining_bytes());
        if b.remaining() < val_len + varint_sz {
            return Err(CorruptedObject);
        }
        b.incr_offset(varint_sz);
        let value = object::Value::from_bytes(b.get_slice(val_len));
        
        Ok(object::KVEntry {
            value,
            revision: object::Revision(u),
            timestamp: hlc::Timestamp::from(ts),
            locked_to_transaction: None
        })
    };

    let mut b = RawData::new(buf);

    let mask = b.get_u8();

    let min = if mask & 1 << 3 != 0 {
        Some(get_key(&mut b)?)
    } else {
        None
    };
    let max = if mask & 1 << 2 != 0 {
        Some(get_key(&mut b)?)
    } else {
        None
    };
    let left = if mask & 1 << 1 != 0 {
        Some(get_key(&mut b)?)
    } else {
        None
    };
    let right = if mask & 1 << 0 != 0 {
        Some(get_key(&mut b)?)
    } else {
        None
    };

    let mut content = HashMap::new();
    
    while b.remaining() != 0 {
        let key = get_key(&mut b)?;
        let entry = get_entry(&mut b)?;
        content.insert(key, entry);
    }

    Ok(object::KVObjectState {
        min,
        max,
        left,
        right,
        content,
        no_existence_locks: HashSet::new()
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use crate::object;
    use crate::hlc;
    use super::*;

    #[test]
    fn full() {
        let rid = uuid::Uuid::parse_str("01cccd1b-e34e-4193-ad62-868a964eab9c").unwrap();
        let mut content = HashMap::new();
        content.insert(
            object::Key::from_bytes(&[8u8, 9u8]),
            object::KVEntry {
                value: object::Value::from_bytes(&[0xau8, 0xbu8]),
                revision: object::Revision(rid),
                timestamp: hlc::Timestamp::from(2),
                locked_to_transaction: None
            }
        );
        let s = object::KVObjectState {
            min:   Some(object::Key::from_bytes(&[0u8, 1u8])),
            max:   Some(object::Key::from_bytes(&[2u8, 3u8])),
            left:  Some(object::Key::from_bytes(&[4u8, 5u8])),
            right: Some(object::Key::from_bytes(&[6u8, 7u8])),
            content,
            no_existence_locks: HashSet::new()
        };

        let b = encode(&s);

        let r = decode(&b[..]).unwrap();

        assert_eq!(r, s);
    }
}