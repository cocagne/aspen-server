use super::*;

pub(super) struct BufferManager {
    transactions: HashMap<TxId, RefCell<Tx>>,
    allocations: HashMap<TxId, RefCell<Alloc>>,
    processing_buffer: RefCell<EntryBuffer>,
    current_buffer: RefCell<EntryBuffer>,
    entry_window_size: usize,
    next_entry_serial: LogEntrySerialNumber,
    last_entry_location: FileLocation,
    earliest_entry_needed: LogEntrySerialNumber
}

impl BufferManager {

    pub(super) fn new<T: Stream>(
        streams: &Vec<T>, 
        entry_window_size: usize,
        recovered_transactions: &Vec<RecoveredTx>,
        recovered_allocations: &Vec<RecoveredAlloc>) -> BufferManager {

        let mut last_serial = LogEntrySerialNumber(0);
        let mut last_location = FileLocation {
            file_id: FileId(0u16),
            offset: 0u64,
            length: 0u32
        };

        for s in streams {
            if let Some((serial, loc)) = s.last_entry() {
                if serial > last_serial {
                    last_serial = serial;
                    last_location = loc;
                }
            }
        }

        let mut transactions = HashMap::new();
        let mut allocations = HashMap::new();

        for rtx in recovered_transactions {
            transactions.insert(rtx.id.clone(), RefCell::new(Tx {
                id: rtx.id.1.clone(),
                txd_location: Some(rtx.txd_location),
                data_locations: Some(rtx.update_locations.iter().map(|ou| ou.1).collect()),
                state: TransactionRecoveryState {
                    store_id: rtx.id.0,
                    serialized_transaction_description: rtx.serialized_transaction_description.clone(),
                    object_updates: rtx.object_updates.clone(),
                    tx_disposition: rtx.tx_disposition,
                    paxos_state: rtx.paxos_state
                },
                last_entry_serial: rtx.last_entry_serial
            }));
        }

        for ra in recovered_allocations {
            allocations.insert(ra.id.clone(), RefCell::new(Alloc {
                data_location: Some(ra.data_location),
                state: AllocationRecoveryState {
                    store_id: ra.id.0,
                    store_pointer: ra.store_pointer.clone(),
                    id: ra.object_id,
                    kind: ra.kind,
                    size: ra.size,
                    data: ArcDataSlice::from(ra.data.clone()),
                    refcount: ra.refcount,
                    timestamp: ra.timestamp,
                    allocation_transaction_id: ra.id.1,
                    serialized_revision_guard: ra.serialized_revision_guard.clone()
                },
                last_entry_serial: ra.last_entry_serial
            }));
        }

        let earliest_entry_needed = transactions.iter().map(|(_,v)| v.borrow().last_entry_serial).chain(
            allocations.iter().map(|(_,v)| v.borrow().last_entry_serial)
        ).fold(last_serial.next(), |a, s| {
            if s < a {
                s
            } else {
                a
            }
        });
       
        BufferManager {
            transactions: transactions,
            allocations: allocations,
            processing_buffer: RefCell::new(EntryBuffer::new()),
            current_buffer: RefCell::new(EntryBuffer::new()),
            entry_window_size,
            next_entry_serial: last_serial.next(),
            last_entry_location: last_location,
            earliest_entry_needed
        }
    }

    pub fn is_empty(&self) -> bool {
        self.current_buffer.borrow().is_empty()
    }

    pub fn add_transaction(&mut self,
        store_id: store::Id,
        transaction_id: transaction::Id,
        serialized_transaction_description: ArcData,
        object_updates: Vec<transaction::ObjectUpdate>,
        tx_disposition: transaction::Disposition,
        paxos_state: paxos::PersistentState,
        last_entry_serial: Option<LogEntrySerialNumber>) {
        
        let txid = TxId(store_id, transaction_id);

        self.transactions.insert(txid.clone(), RefCell::new(Tx {
            id: transaction_id,
            txd_location: None,
            data_locations: None,
            state: TransactionRecoveryState {
                store_id,
                serialized_transaction_description,
                object_updates,
                tx_disposition,
                paxos_state
            },
            last_entry_serial: last_entry_serial.unwrap_or(self.next_entry_serial)
        }));

        self.current_buffer.borrow_mut().add_transaction(txid);
    }

    pub fn update_transaction(&mut self,
        store_id: store::Id,
        transaction_id: transaction::Id,
        tx_disposition: transaction::Disposition,
        paxos_state: paxos::PersistentState,
        object_updates: Option<Vec<transaction::ObjectUpdate>>){
        
        let txid = TxId(store_id, transaction_id);

        if let Some(tx) = self.transactions.get(&txid) {
            let mut mtx = tx.borrow_mut();

            mtx.state.tx_disposition = tx_disposition;
            mtx.state.paxos_state = paxos_state;

            if let Some(updates) = object_updates {
                mtx.state.object_updates = updates;
                mtx.data_locations = None;
            }

            self.current_buffer.borrow_mut().add_transaction(txid);
        };
    }

    pub fn drop_transaction(&mut self, store_id: store::Id, transaction_id: transaction::Id) {
        let tx_id = TxId(store_id, transaction_id);
        self.current_buffer.borrow_mut().drop_transaction(tx_id);
        self.transactions.remove(&tx_id);
    }

    pub fn drop_allocation(&mut self, store_id: store::Id, transaction_id: transaction::Id) {
        let tx_id = TxId(store_id, transaction_id);
        self.current_buffer.borrow_mut().drop_allocation(tx_id);
        self.allocations.remove(&tx_id);
    }

    pub fn add_allocation(&mut self,
        store_id: store::Id,
        store_pointer: store::Pointer,
        id: object::Id,
        kind: object::Kind,
        size: Option<u32>,
        data: ArcDataSlice,
        refcount: object::Refcount,
        timestamp: hlc::Timestamp,
        allocation_transaction_id: transaction::Id,
        serialized_revision_guard: ArcDataSlice,
        last_entry_serial: Option<LogEntrySerialNumber>) {

        let txid = TxId(store_id, allocation_transaction_id);

        self.allocations.insert(txid.clone(), RefCell::new(Alloc {
            data_location: None,
            state: AllocationRecoveryState {
                store_id,
                store_pointer,
                id,
                kind,
                size,
                data,
                refcount,
                timestamp,
                allocation_transaction_id,
                serialized_revision_guard
            },
            last_entry_serial: last_entry_serial.unwrap_or(self.next_entry_serial)
        }));

        self.current_buffer.borrow_mut().add_allocation(txid);
    }

    fn prune_data_stored_in_file(&self, file_id: FileId) {
        let mut current_buffer = self.current_buffer.borrow_mut();

        for (tx_id, tx) in &self.transactions {
            let mut mtx = tx.borrow_mut();

            if let Some(l) = mtx.txd_location.as_ref() {
                if l.file_id == file_id {
                    mtx.txd_location = None;
                    current_buffer.add_transaction(*tx_id);
                }

                if let Some(data_locations) = mtx.data_locations.as_ref() {
                    for l in data_locations {
                        if l.file_id == file_id {
                            mtx.data_locations = None;
                            current_buffer.add_transaction(*tx_id);
                            break;
                        }
                    }
                }
            }
        }

        for (tx_id, a) in &self.allocations {
            let mut ma = a.borrow_mut();

            if let Some(l) = ma.data_location.as_ref() {
                if l.file_id == file_id {
                    ma.data_location = None;
                    current_buffer.add_allocation(*tx_id);
                }
            }
        }
    }

    pub fn log_entry<T: Stream>(&mut self, stream: &T) {
        
        std::mem::swap(&mut self.processing_buffer, &mut self.current_buffer);
        
        let buf = &self.processing_buffer.borrow();

        let entry_serial = self.next_entry_serial;

        self.next_entry_serial = self.next_entry_serial.next();

        // If this entry falls on a window-size boundary, put all transactions and allocations
        // written behind the window-size into the entry buffer
        if entry_serial.0 % self.entry_window_size as u64 == 0 {
            let earliest = LogEntrySerialNumber(entry_serial.0 - self.entry_window_size as u64);

            for (txid, tx) in &self.transactions {
                if tx.borrow().last_entry_serial < earliest {
                    self.current_buffer.borrow_mut().add_transaction(*txid);
                }
            }
            for (txid, a) in &self.allocations {
                if a.borrow().last_entry_serial < earliest {
                    self.current_buffer.borrow_mut().add_allocation(*txid);
                }
            }

            self.earliest_entry_needed = earliest;
        }

        let mut txs: Vec<&RefCell<Tx>> = Vec::new();
        let mut allocs: Vec<&RefCell<Alloc>> = Vec::new();

        for txid in &buf.tx_set {
            self.transactions.get(&txid).map( |tx| txs.push(tx) );
        }

        for txid in &buf.alloc {
            self.allocations.get(&txid).map( |a| allocs.push(a) );
        }

        let (prune_file_from_log, entry_location) = encoding::log_entry(
            entry_serial, self.earliest_entry_needed, self.last_entry_location, &txs, &allocs,
            &buf.tx_deletions, &buf.alloc_deletions, stream
        );

        self.last_entry_location = entry_location;

        drop(buf); // Drop immutable borrow so we can re-borrow as mutable and clear the buffer

        self.processing_buffer.borrow_mut().clear();

        // Prune files at the end of the process to prevent dropping file locations on transactions
        // and allocations going into this entry
        //
        if let Some(prune_file_id) = prune_file_from_log {
            // We've already finalized the buffer for the current log entry. This method will
            // find all tx/allocs that have data stored in the file and enter them into the
            // next buffer
            self.prune_data_stored_in_file(prune_file_id);
        }
    }
}