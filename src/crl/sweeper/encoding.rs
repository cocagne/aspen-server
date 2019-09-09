use super::*;

pub(super) fn log_entry<T: Stream>(
    entry_serial_number: LogEntrySerialNumber,
    earliest_entry_needed: LogEntrySerialNumber,
    last_entry_location: FileLocation,
    txs: &Vec<&RefCell<Tx>>,
    allocs: &Vec<&RefCell<Alloc>>,
    tx_deletions: &Vec<TxId>,
    alloc_deletions: &Vec<TxId>,
    stream: &T) -> (Option<FileId>, FileLocation) {

    let mut prune_file_from_log: Option<FileId> = None;

    let (data_sz, tail_sz, num_data_buffers) = calculate_write_size(txs, allocs, 
        tx_deletions, alloc_deletions);

    let (file_id, file_uuid, initial_offset, padding_sz) = {
        let (file_id, file_uuid, offset, max_size) = stream.status();

        let padding = pad_to_4k_alignment(offset, data_sz, tail_sz);

        if offset + data_sz + padding + tail_sz <= max_size {
            (file_id, file_uuid, offset, padding)
        }
        else {
            prune_file_from_log = stream.rotate_files();

            let (file_id, file_uuid, offset, _) = stream.status();

            (file_id, file_uuid, offset, padding)
        }
    };

    let entry_offset = initial_offset + data_sz + padding_sz;

    let mut offset = initial_offset;

    let mut tail = DataMut::with_capacity((padding_sz + tail_sz) as usize);

    tail.zfill(padding_sz);

    let mut buffers = Vec::<ArcDataSlice>::with_capacity(num_data_buffers + 1);

    let mut push_data_buffer = |b: ArcDataSlice| -> FileLocation {
        let length = b.len();
        let l = FileLocation{file_id : file_id, offset : offset, length : length as u32};
        buffers.push(b);
        offset += length as u64;
        l
    };

    for tx in txs.iter() {
        let mut mtx = tx.borrow_mut();

        mtx.last_entry_serial = entry_serial_number;

        if mtx.txd_location.is_none() {
            let s = ArcDataSlice::from(&mtx.state.serialized_transaction_description);
            mtx.txd_location = Some(push_data_buffer(s));
        }

        if mtx.data_locations.is_none() && !mtx.state.object_updates.is_empty() {
            let mut v = Vec::new();
            for ou in &mtx.state.object_updates {
                v.push(push_data_buffer(ou.data.clone()));
            }
            mtx.data_locations = Some(v);
        }

        drop(mtx);

        encode_tx_state(&tx.borrow(), &mut tail);
    }

    for a in allocs.iter() {
        let mut ma = a.borrow_mut();

        ma.last_entry_serial = entry_serial_number;

        if ma.data_location.is_none() {
            ma.data_location = Some(push_data_buffer(ma.state.data.clone()));
        }

        drop(ma);

        encode_alloc_state(&a.borrow(), &mut tail);
    }

    for id in tx_deletions {
        id.encode_into(&mut tail);   
    }

    for id in alloc_deletions {
        id.encode_into(&mut tail);
    }

    // ---------- Static Entry Block ----------

    assert_eq!((tail.capacity() - tail.len()) as u64, STATIC_ENTRY_SIZE);

    let entry_block_offset = initial_offset + data_sz + tail.offset() as u64;

    // Entry block always ends with:
    //   entry_serial_number - 8
    //   entry_begin_offset - 8
    //   earliest_needed_entry_serial_number - 8
    //   num_transactions - 4
    //   num_allocations - 4
    //   num_tx_deletions - 4
    //   num_alloc_deletions - 4
    //   prev_entry_file_location - 14 (2 + 8 + 4)
    //   file_uuid - 16
    tail.put_u64_le(entry_serial_number.0);
    tail.put_u64_le(entry_offset);
    tail.put_u64_le(earliest_entry_needed.0);
    tail.put_u32_le(txs.len() as u32);
    tail.put_u32_le(allocs.len() as u32);
    tail.put_u32_le(tx_deletions.len() as u32);
    tail.put_u32_le(alloc_deletions.len() as u32);
    last_entry_location.encode_into(&mut tail);
    tail.put_slice(file_uuid.as_bytes());

    buffers.push(ArcDataSlice::from(tail.finalize()));

    stream.write(buffers, entry_serial_number);

    let entry_location = FileLocation{
        file_id : file_id, 
        offset : entry_block_offset, 
        length : STATIC_ENTRY_SIZE as u32
    };

    (prune_file_from_log, entry_location)
}

// pub struct TransactionRecoveryState {
//     store_id: store::Id, 17
//     transaction_id: 16
//     serialized_transaction_description: Bytes, 14 (FileLocation)
//     tx_disposition: transaction::Disposition, 1
//     paxos_state: paxos::PersistentState, 11 (1:mask-byte + 5:proposalId + 5:proposalId)
//     object_updates: Vec<transaction::ObjectUpdate>, 4:count + num_updates * (16:objuuid + FileLocation)
// }
fn decode_tx_state(buf: &mut Data, entry_serial: LogEntrySerialNumber) -> Result<RecoveringTx, DecodeError> {
    if buf.remaining() < STATIC_TX_SIZE as usize {
        Err(DecodeError{})
    } else {
        let id = TxId::decode_from(buf);
        let txd_loc = FileLocation::decode_from(buf);
        let disposition = transaction::Disposition::from_u8(buf.get_u8())?;
        let mask = buf.get_u8();
        let promise_peer = buf.get_u8();
        let promise_proposal_id = buf.get_u32_le();
        let accepted_peer = buf.get_u8();
        let accepted_proposal_id = buf.get_u32_le();

        let promised = if mask & 1 << 2 == 0 {
            None
        } else {
            Some(paxos::ProposalId{
                number: promise_proposal_id,
                peer: promise_peer
            })
        };

        let accepted = if mask & 1 << 1 == 0 {
            None
        } else {
            let prop_id = paxos::ProposalId {
                number: accepted_proposal_id,
                peer: accepted_peer
            };

            let have_accepted = mask & 1 << 0 != 0;

            Some((prop_id, have_accepted))
        };

        let pax = paxos::PersistentState{
            promised,
            accepted
        };

        let mut updates: Vec<(uuid::Uuid, FileLocation)> = Vec::new();
        let nupdates = buf.get_u32_le();

        if buf.remaining() < nupdates as usize * (16 + FILE_LOCATION_SIZE as usize) {
            return Err(DecodeError{});
        }

        for _ in 0 .. nupdates {
            let mut uuid_bytes: [u8; 16] = [0; 16];
            buf.copy_to_slice(&mut uuid_bytes);
            let location = FileLocation::decode_from(buf);
            updates.push((uuid::Uuid::from_bytes(uuid_bytes), location));
        }

        Ok(RecoveringTx{
            id,
            serialized_transaction_description: txd_loc,
            object_updates: updates,
            tx_disposition: disposition,
            paxos_state: pax,
            last_entry_serial: entry_serial
        })
    }
}



fn encode_tx_state(tx: &Tx, buf: &mut DataMut) {
    let tx_id = TxId(tx.state.store_id, tx.id);
    tx_id.encode_into(buf);
    
    if let Some(loc) = &tx.txd_location {
        loc.encode_into(buf);
    } else {
        FileLocation::null().encode_into(buf);
    }

    buf.put_u8(tx.state.tx_disposition.to_u8());
    let mut mask = 0u8;
    let mut promise_peer = 0u8;
    let mut promise_proposal_id = 0u32;
    let mut accepted_peer = 0u8;
    let mut accepted_proposal_id = 0u32;
    if let Some(promise) = tx.state.paxos_state.promised {
        mask |= 1 << 2;
        promise_peer = promise.peer;
        promise_proposal_id = promise.number;
    }
    if let Some((prop_id, accepted)) = tx.state.paxos_state.accepted {
        mask |= 1 << 1;
        if accepted {
            mask |= 1 << 0;
        }
        accepted_peer = prop_id.peer;
        accepted_proposal_id = prop_id.number;
    }
    buf.put_u8(mask);
    buf.put_u8(promise_peer);
    buf.put_u32_le(promise_proposal_id);
    buf.put_u8(accepted_peer);
    buf.put_u32_le(accepted_proposal_id);

    match &tx.data_locations {
        None => buf.put_u32_le(0u32),

        Some(dl) => {
            buf.put_u32_le(dl.len() as u32);
            for (ou, loc) in tx.state.object_updates.iter().zip(dl.iter()) {
                buf.put_slice(ou.object_id.0.as_bytes());
                loc.encode_into(buf);
            }
        }
    }
}



fn decode_alloc_state(buf: &mut Data, entry_serial: LogEntrySerialNumber) -> Result<RecoveringAlloc, DecodeError> {
    if buf.remaining() < STATIC_ARS_SIZE as usize {
        Err(DecodeError{})
    } else {
        let id = TxId::decode_from(buf);

        //let txd_loc = FileLocation::decode_from(buf);

        let object_id = object::Id(buf.get_uuid());
        let kind = object::Kind::from_u8(buf.get_u8())?;

        let size = {
            let sz = buf.get_u32_le();

            if sz == 0 {
                None
            } else {
                Some(sz)
            }
        };

        let data = FileLocation::decode_from(buf);

        let refcount = {
            let update_serial = buf.get_u32_le();
            let count = buf.get_u32_le();
            object::Refcount {
                update_serial,
                count
            }
        };

        let timestamp = hlc::Timestamp::from(buf.get_u64_le());

        let serialized_revision_guard = {
            let nbytes = buf.get_u32_le() as usize;
            if buf.remaining() < nbytes {
                return Err(DecodeError{});
            }
            let mut v: Vec<u8> = Vec::with_capacity(nbytes);
            v.extend_from_slice(buf.get_slice(nbytes));
            ArcDataSlice::from(v)
        };

        let store_pointer = {
            if buf.remaining() < 4 {
                return Err(DecodeError{});
            }

            let nbytes = buf.get_u32_le() as usize;

            if buf.remaining() < nbytes {
                return Err(DecodeError{});
            }

            if nbytes == 0 {
                store::Pointer::None
            } 
            else if nbytes <= 23 {
                let mut content: [u8; 23] = [0; 23];

                let s = buf.get_slice(nbytes);

                for (idx, byte) in s.iter().enumerate() {
                    content[idx] = *byte;
                }
                
                store::Pointer::Short {
                    nbytes: nbytes as u8,
                    content
                }
            } 
            else {
                let mut content: Vec<u8> = Vec::with_capacity(nbytes);
                content.extend_from_slice(buf.get_slice(nbytes));
                store::Pointer::Long {
                    content
                }
            }
        };

        Ok(RecoveringAlloc{
            id,
            store_pointer,
            object_id,
            kind,
            size,
            data,
            refcount,
            timestamp,
            serialized_revision_guard,
            last_entry_serial: entry_serial
        })
    }
}

fn encode_alloc_state(a: &Alloc, buf: &mut DataMut) {
    assert!(a.data_location.is_some(), "DataLocation field must be set!");

    let tx_id = TxId(a.state.store_id, a.state.allocation_transaction_id);
    tx_id.encode_into(buf);

    buf.put_uuid(a.state.id.0);
    buf.put_u8(a.state.kind.to_u8());
    buf.put_u32_le(match a.state.size {
        None => 0u32,
        Some(len) => len
    });
    
    a.data_location.unwrap().encode_into(buf);

    buf.put_u32_le(a.state.refcount.update_serial);
    buf.put_u32_le(a.state.refcount.count);
    buf.put_u64_le(a.state.timestamp.to_u64());
    buf.put_u32_le(a.state.serialized_revision_guard.len() as u32);
    buf.put_slice(&a.state.serialized_revision_guard.as_bytes());

    match &a.state.store_pointer {
        store::Pointer::None => buf.put_u32_le(0),
        store::Pointer::Short{nbytes, content} => {
            buf.put_u32_le(*nbytes as u32);
            buf.put_slice(&content[0..*(nbytes) as usize]);
        },
        store::Pointer::Long{content} => {
            buf.put_u32_le(content.len() as u32);
            buf.put_slice(content);
        }
    };
}



/// Calculates the size required for the write.
/// 
/// Returns (size-of-pre-entry-data, 4k-alignment-padding-bytes, size-of-entry-block, number-of-data-buffers)
fn calculate_write_size(
    txs: &Vec<&RefCell<Tx>>, 
    allocs: &Vec<&RefCell<Alloc>>,
    tx_deletions: &Vec<TxId>,
    alloc_deletions: &Vec<TxId>) -> (u64, u64, usize) {

    let mut update_count: u64 = 0;
    let mut buffer_count: usize = 0;
    let mut data: u64 = 0;
    let mut tail: u64 = STATIC_ENTRY_SIZE; 

    for tx in txs {
        let tx = tx.borrow();

        if tx.txd_location.is_none() {
            data += tx.state.serialized_transaction_description.len() as u64;
            buffer_count += 1;
        }
        if tx.data_locations.is_none() && ! tx.state.object_updates.is_empty() {
            for ou in &tx.state.object_updates {
                data += ou.data.len() as u64;
                update_count += 1;
                buffer_count += 1;
            }
        }
    }

    // Update format is 16-byte UUID + 14-byte FileLocation
    tail += txs.len() as u64 * STATIC_TX_SIZE + update_count * (16 + FILE_LOCATION_SIZE);

    tail += allocs.len() as u64 * STATIC_ARS_SIZE;

    for a in allocs {
        let a = a.borrow();

        if a.data_location.is_none() {
            data += a.state.data.len() as u64;
            buffer_count += 1;
        }
        
        tail += a.state.store_pointer.len() as u64 + a.state.serialized_revision_guard.len() as u64;
    }

    tail += tx_deletions.len() as u64 * TXID_SIZE;
    tail += alloc_deletions.len() as u64 * TXID_SIZE;

    (data, tail, buffer_count)
}

fn pad_to_4k_alignment(offset: u64, data_size: u64, tail_size: u64) -> u64 {
    let base = offset + data_size + tail_size;
    if base < 4096 {
        4096 - base
    } else {
        let remainder = base % 4096;
        if remainder == 0 {
            0
        } else {
            4096 - remainder
        }
    }
}

#[cfg(test)]
mod tests {
    
    use super::*;
    use crate::data::*;
    use crate::store;
    use crate::transaction;
    use crate::paxos;

    fn uuids() -> [uuid::Uuid; 4] {
        [uuid::Uuid::parse_str("d1cccd1b-e34e-4193-ad62-868a964eab9c").unwrap(),
        uuid::Uuid::parse_str("f308d237-a26e-4735-b001-6782fb2eac38").unwrap(),
        uuid::Uuid::parse_str("0e18b5ad-0717-4a5b-b0d1-e675dd55790a").unwrap(),
        uuid::Uuid::parse_str("7c27c2af-4d7a-4eab-867d-00691d6dfed8").unwrap()]
    }

    #[test]
    fn padding() {
        assert_eq!(pad_to_4k_alignment(0, 0, 16), 4096-16);
        assert_eq!(pad_to_4k_alignment(0, 0, 17), 4096-17);
        assert_eq!(pad_to_4k_alignment(4096, 0, 16), 4096-16);
        assert_eq!(pad_to_4k_alignment(4096, 2048, 2048), 0);
        assert_eq!(pad_to_4k_alignment(4096, 4096, 4096), 0);
        assert_eq!(pad_to_4k_alignment(0, 4096, 4096), 0);
    }

    #[test]
    fn tx_minimal_encoding() {
        let ids = uuids();
        let txid = transaction::Id(ids[0]);
        let pool_uuid = ids[1];
        let store_id = store::Id { pool_uuid, pool_index: 1u8 };
        let std = ArcData::from(vec![1u8,2u8,3u8]);
        let tx_disposition = transaction::Disposition::VoteCommit;
        let paxos_state = paxos::PersistentState { promised: None, accepted: None };
        let tx1 = Tx {
            id: txid,
            txd_location: None,
            data_locations: None,
            state: TransactionRecoveryState {
                store_id,
                serialized_transaction_description: std,
                object_updates: Vec::new(),
                tx_disposition,
                paxos_state
            },
            last_entry_serial: LogEntrySerialNumber(0)
        };
        
        let mut m = DataMut::with_capacity(4096);

        encode_tx_state(&tx1, &mut m);

        m.set_offset(0);

        let mut r = Data::from(m);

        let ra = decode_tx_state(&mut r, LogEntrySerialNumber(0)).unwrap();

        let expected = RecoveringTx {
            id: TxId(store_id, txid),
            serialized_transaction_description: FileLocation::null(),
            object_updates: Vec::new(),
            tx_disposition,
            paxos_state,
            last_entry_serial: LogEntrySerialNumber(0)
        };

        assert_eq!(ra, expected);
    }

    #[test]
    fn tx_full_encoding() {
        let ids = uuids();
        let txid = transaction::Id(ids[0]);
        let txd_loc = FileLocation { file_id: FileId(5), offset: 2,length: 3 };
        let u1 = FileLocation { file_id: FileId(1), offset: 1, length: 1 };
        let u2 = FileLocation { file_id: FileId(2), offset: 2, length: 2 };

        let oid1 = object::Id(ids[2]);
        let oid2 = object::Id(ids[3]);
        let ads = ArcDataSlice::from(vec![0u8]);

        let ou1 = transaction::ObjectUpdate { object_id: oid1, data: ads.clone() };
        let ou2 = transaction::ObjectUpdate { object_id: oid2, data: ads.clone() };

        let pool_uuid = ids[1];
        let store_id = store::Id { pool_uuid, pool_index: 1u8 };
        let std = ArcData::from(vec![1u8,2u8,3u8]);
        let tx_disposition = transaction::Disposition::VoteCommit;
        let pid1 = paxos::ProposalId { number: 1, peer: 2 };
        let pid2 = paxos::ProposalId { number: 3, peer: 3 };
        let paxos_state = paxos::PersistentState { promised: Some(pid1), accepted: Some((pid2, true)) };

        let tx1 = Tx {
            id: txid,
            txd_location: Some(txd_loc),
            data_locations: Some(vec![u1, u2]),
            state: TransactionRecoveryState {
                store_id,
                serialized_transaction_description: std,
                object_updates: vec![ou1, ou2],
                tx_disposition,
                paxos_state
            },
            last_entry_serial: LogEntrySerialNumber(0)
        };
        
        let mut m = DataMut::with_capacity(4096);

        encode_tx_state(&tx1, &mut m);

        m.set_offset(0);

        let mut r = Data::from(m);

        let ra = decode_tx_state(&mut r, LogEntrySerialNumber(0)).unwrap();

        let expected = RecoveringTx {
            id: TxId(store_id, txid),
            serialized_transaction_description: txd_loc,
            object_updates: vec![(ids[2], u1), (ids[3], u2)],
            tx_disposition,
            paxos_state,
            last_entry_serial: LogEntrySerialNumber(0)
        };

        assert_eq!(ra, expected);
    }

    #[test]
    fn alloc_encoding() {
        let data_location = FileLocation { file_id: FileId(1), offset: 2,length: 3 };
        let ids = uuids();
        let txid = transaction::Id(ids[0]);
        let mut sp_content: [u8; 23] = [0u8; 23];
        sp_content[0] = 2;
        sp_content[1] = 2;
        sp_content[2] = 2;
        let sp_content = sp_content;
        let store_pointer = store::Pointer::Short{nbytes: 3, content: sp_content};
        let object_id = object::Id(ids[1]);
        let kind = object::Kind::KeyValue;
        let size = Some(10);
        let data = ArcDataSlice::from(vec![0u8]);
        let rc = object::Refcount { update_serial: 5, count: 2 };
        let timestamp = hlc::Timestamp::from(10u64);
        let allocation_transaction_id = txid;
        let srg = ArcDataSlice::from(vec![0u8, 1u8, 2u8]);
        let pool_uuid = ids[2];
        let store_id = store::Id { pool_uuid, pool_index: 1u8 };
        
        let alloc = Alloc {
            data_location: Some(data_location),
            state: AllocationRecoveryState {
                store_id,
                store_pointer: store_pointer.clone(),
                id: object_id,
                kind,
                size,
                data: data.clone(),
                refcount: rc,
                timestamp,
                allocation_transaction_id,
                serialized_revision_guard: srg.clone()
            },
            last_entry_serial: LogEntrySerialNumber(0)
        };
        
        let mut m = DataMut::with_capacity(4096);

        encode_alloc_state(&alloc, &mut m);

        m.set_offset(0);

        let mut r = Data::from(m);

        let ra = decode_alloc_state(&mut r, LogEntrySerialNumber(0)).unwrap();

        let expected = RecoveringAlloc {
            id: TxId(store_id, txid),
            store_pointer,
            object_id,
            kind,
            size,
            data: data_location,
            refcount: rc,
            timestamp,
            serialized_revision_guard: srg.clone(),
            last_entry_serial: LogEntrySerialNumber(0)
        };

        assert_eq!(ra, expected);
    }
}