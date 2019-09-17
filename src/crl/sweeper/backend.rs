
use std::sync::{Mutex, Arc};

use crossbeam::crossbeam_channel;
use crossbeam::crossbeam_channel::TryRecvError;

use crate::crl::RequestCompletionHandler;
use super::*;

// Used to indicate that an entry is full and cannot accept any more data
struct EntryFull;

pub(self) struct LogState {
    receiver: crossbeam_channel::Receiver<Request>,
    entry_window_size: usize,
    transactions: HashMap<TxId, RefCell<Tx>>,
    allocations: HashMap<TxId, RefCell<Alloc>>,
    next_entry_serial: LogEntrySerialNumber,
    last_entry_location: FileLocation,
    earliest_entry_needed: LogEntrySerialNumber,

    completion_handlers: Vec<Arc<dyn RequestCompletionHandler>>,
    last_notified_serial: LogEntrySerialNumber,

    pending_completions: HashMap<LogEntrySerialNumber, Vec<Completion>>,

    /// Channels do not have a peek option so if we cannot add a request to the current entry
    /// it will be placed here for the next entry to consume
    next_request: Option<Request>
}

impl LogState {

    pub(super) fn new(
        receiver: crossbeam_channel::Receiver<Request>,
        entry_window_size: usize, // ensures we never need to read more than window_size entries during recovery
        recovered_transactions: &Vec<RecoveredTx>,
        recovered_allocations: &Vec<RecoveredAlloc>,
        last_entry_serial: LogEntrySerialNumber,
        last_entry_location: FileLocation) -> Arc<Mutex<LogState>> {

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
        ).fold(last_entry_serial.next(), |a, s| {
            if s < a {
                s
            } else {
                a
            }
        });

        Arc::new( Mutex::new( LogState {
            receiver,
            entry_window_size,
            transactions,
            allocations,
            next_entry_serial: last_entry_serial.next(),
            last_entry_location: last_entry_location,
            earliest_entry_needed,
            completion_handlers: Vec::new(),
            pending_completions: HashMap::new(),
            last_notified_serial: last_entry_serial,
            next_request: None
        }))
    }

    fn prune_data_stored_in_file(
        &mut self,
        file_id: FileId, 
        entry: &mut Entry) -> Result<(), EntryFull> {

        for (tx_id, tx) in &self.transactions {
            let mut mtx = tx.borrow_mut();

            let add = match mtx.txd_location.as_ref() {
                Some(loc) => {
                    let mut add_it = false;

                    if loc.file_id == file_id {
                        mtx.txd_location = None;
                        add_it = true;    
                    } 

                    if let Some(locations) = &mtx.data_locations {
                        for l in locations {
                            if l.file_id == file_id {
                                mtx.data_locations = None;
                                add_it = true;
                                break;
                            }
                        }
                    }

                    add_it
                },
                None => false
            };

            drop(mtx);

            if add {
                entry.add_transaction(tx_id, tx, None)?;
            }
        }

        for (tx_id, a) in &self.allocations {
            let mut ma = a.borrow_mut();

            let add = match ma.data_location.as_ref() {
                None => false,
                Some(l) => {
                    if l.file_id == file_id {
                        ma.data_location = None;
                        true
                    } else {
                        false
                    }
                }
            };

            drop(ma);

            if add {
                entry.add_allocation(tx_id, a, None);
            }
        }

        Ok(())
    }
    
    fn add_request(&mut self, 
        entry: &mut Entry, 
        request: &Request) -> RequestResult {
        
        // don't have to create it again at least
        // Check entry first. If we can't add it to the entry, we don't need to handle
        // the request (it'll just be re-attempted later)
        match request {
            Request::SaveTransactionState {
                client_request,
                store_id,
                transaction_id,
                serialized_transaction_description,
                object_updates,
                tx_disposition,
                paxos_state
            } => {
                let txid = TxId(*store_id, *transaction_id);

                match self.transactions.get(&txid) {
                    Some(tx) => {
                        let mut mtx = tx.borrow_mut();

                        if mtx.state.object_updates.len() == 0 && object_updates.len() != 0 {
                            mtx.state.object_updates = object_updates.clone();
                            mtx.data_locations = None;
                            mtx.state.tx_disposition = *tx_disposition;
                            mtx.state.paxos_state = *paxos_state;
                        }

                        drop(mtx);

                        match entry.add_transaction(&txid, &tx, Some(client_request)) {
                            Ok(_) => RequestResult::Okay,
                            Err(_) => RequestResult::EntryIsFull
                        }
                    },
                    None => {
                        let tx = RefCell::new(Tx {
                            id: *transaction_id,
                            txd_location: None,
                            data_locations: None,
                            state: TransactionRecoveryState {
                                store_id: *store_id,
                                serialized_transaction_description: serialized_transaction_description.clone(),
                                object_updates: object_updates.clone(),
                                tx_disposition: *tx_disposition,
                                paxos_state: *paxos_state
                            },
                            last_entry_serial: self.next_entry_serial
                        });

                        match entry.add_transaction(&txid, &tx, Some(client_request)) {
                            Ok(_) => {
                                self.transactions.insert(txid, tx);
                                RequestResult::Okay
                            },
                            Err(_) => RequestResult::EntryIsFull
                        }
                    }
                }
            },
            Request::DropTransactionData {
                store_id,
                transaction_id,
            } => {
                let txid = TxId(*store_id, *transaction_id);
                if let Some(tx) = self.transactions.get(&txid) {
                    let mut mtx = tx.borrow_mut();
                    mtx.data_locations = None;
                    mtx.state.object_updates.clear();
                };
                RequestResult::Okay
            },
            Request::DeleteTransactionState {
                store_id,
                transaction_id,
            } => {
                let txid = TxId(*store_id, *transaction_id);
                match entry.drop_transaction(&txid) {
                    Ok(_) => {
                        self.transactions.remove(&txid);
                        RequestResult::Okay
                    },
                    Err(_) => RequestResult::EntryIsFull
                }
            },
            Request::SaveAllocationState {
                client_request,
                state
            } => {
                let a = RefCell::new(Alloc{
                    data_location: None,
                    state: state.clone(),
                    last_entry_serial: self.next_entry_serial
                });
                let txid = TxId(state.store_id, state.allocation_transaction_id);
                match entry.add_allocation(&txid, &a, Some(client_request)) {
                    Ok(_) => {
                        self.allocations.insert(txid, a);
                        RequestResult::Okay
                    },
                    Err(_) => RequestResult::EntryIsFull
                }
            },
            Request::DeleteAllocationState {
                store_id,
                allocation_transaction_id,
            } => {
                let txid = TxId(*store_id, *allocation_transaction_id);
                match entry.drop_allocation(&txid) {
                    Ok(_) => {
                        self.allocations.remove(&txid);
                        RequestResult::Okay
                    },
                    Err(_) => RequestResult::EntryIsFull
                }
            },
            Request::GetFullRecoveryState {
                store_id,
                sender
            } => {
                let mut txs: Vec<TransactionRecoveryState> = Vec::new();
                let mut allocs:  Vec<AllocationRecoveryState> = Vec::new();

                for (_, tx) in &self.transactions {
                    let tx = tx.borrow();
                    if tx.state.store_id == *store_id {
                        txs.push(tx.state.clone());
                    }
                }

                for (_, a) in &self.allocations {
                    let a = a.borrow();
                    if a.state.store_id == *store_id {
                        allocs.push(a.state.clone());
                    }
                }

                sender.send(FullStateResponse(txs, allocs));
                RequestResult::Okay   
            },
            Request::RegisterClientRequest {
                sender,
                handler
            } => {
                let client_id = ClientId(self.completion_handlers.len());
                self.completion_handlers.push(handler.clone());
                sender.send(RegisterClientResponse { client_id });
                RequestResult::Okay
            }
        }
    }

    /// Moves transactions and allocation descriptions behind the entry window size into
    /// the current entry. The goal here is to restrict the number of entries that must be
    /// read during crash recovery. Returns true if all necessary entries have been migrated
    /// forwad. False otherwise.
    fn migrate_earliest_entry(&mut self, entry: &mut Entry) -> bool {
    
        // If this entry falls on a window-size boundary, put all transactions and allocations
        // written behind the window-size into the entry buffer
        if self.next_entry_serial.0 % self.entry_window_size as u64 != 0 {
            true // nothing to do
        } else {
            let earliest = self.next_entry_serial.0 as u64 - self.entry_window_size as u64;
            let earliest = LogEntrySerialNumber(earliest);

            for (txid, tx) in &self.transactions {
                if tx.borrow().last_entry_serial < earliest {
                    match entry.add_transaction(txid, tx, None) {
                        Ok(_) => (),
                        Err(_) => return false
                    }
                }
            }
            for (txid, a) in &self.allocations {
                if a.borrow().last_entry_serial < earliest {
                     match entry.add_allocation(txid, a, None) {
                        Ok(_) => (),
                        Err(_) => return false
                    }
                }
            }

            self.earliest_entry_needed = earliest;

            true
        } 
    }

    /// Creates the log entry and returns it as a vector of ArcDataSlice.
    /// 
    /// Returns: (Optional EntrySerialNumber, FileLocation of this entry, entry data to be passed to the stream)
    pub fn create_log_entry(&mut self, 
        entry: &mut Entry,
        stream: &Box<&mut dyn FileStream>) -> (LogEntrySerialNumber, Vec::<ArcDataSlice>) {
        
        let mut txs: Vec<&RefCell<Tx>> = Vec::new();
        let mut allocs: Vec<&RefCell<Alloc>> = Vec::new();

        for txid in &entry.tx_set {
            self.transactions.get(&txid).map( |tx| txs.push(tx) );
        }

        for txid in &entry.allocs {
            self.allocations.get(&txid).map( |a| allocs.push(a) );
        }

        let entry_serial = self.next_entry_serial;

        let (entry_location, buffers) = encoding::log_entry(
            entry_serial, self.earliest_entry_needed, self.last_entry_location,
            &txs, &allocs,
            &entry.tx_deletions, &entry.alloc_deletions, stream
        );

        self.last_entry_location = entry_location;
        self.next_entry_serial = self.next_entry_serial.next();
        
        (entry_serial, buffers)
    }
}


enum RequestResult {
    Okay,
    EntryIsFull
}

pub struct Backend {
    backend: BackendImpl
}

impl Backend {
    pub fn recover(
        crl_directory: &Path, 
        entry_window_size: usize
        ) -> Result<Backend> {
            let streams = Vec::new();

            let r = log_file::recover(crl_directory, entry_window_size)?;

            
            
            Ok(Backend {
                backend: BackendImpl::new(streams, entry_window_size, &r.transactions,
                  &r.allocations, r.last_entry_serial, r.last_entry_location)
            })
        }
}

struct BackendImpl {
    log_state: Arc<Mutex<LogState>>,
    streams: Vec<Box<dyn FileStream>>,
    pub sender: crossbeam_channel::Sender<Request>,
}

impl BackendImpl {
    pub fn new(
        streams: Vec<Box<dyn FileStream>>,
        entry_window_size: usize, // ensures we never need to read more than window_size entries during recovery
        recovered_transactions: &Vec<RecoveredTx>,
        recovered_allocations: &Vec<RecoveredAlloc>,
        last_entry_serial: LogEntrySerialNumber,
        last_entry_location: FileLocation) -> BackendImpl
    {
        let (sender, receiver) = crossbeam_channel::unbounded();

        let backend = BackendImpl {
            log_state: LogState::new(
                    receiver, entry_window_size, 
                    recovered_transactions, recovered_allocations, 
                    last_entry_serial, last_entry_location),
            streams,
            sender,
        };

        backend
    }

    pub(super) fn clone_sender(&self) -> crossbeam_channel::Sender<Request> {
        self.sender.clone()
    }

    fn io_thread(&self, stream: Box<&mut dyn FileStream>) {
        
        let max_file_size = stream.const_max_file_size();

        let mut prune_file: Option<FileId> = None;
        let mut entry = Entry::new(max_file_size);

        'top_level: loop {
            
            entry.reset(&stream);

            let (prune_required, entry_serial, buffers) = { 
                // ---------- Lock Log State Mutex ----------

                let mut state = self.log_state.lock().unwrap(); // Panic if lock fails

                // First prune all content from our to-be-pruned file, if we have a file to prune.
                // This action alone may entirely fill the entry buffer (and may even take
                // multiple passes).
            
                let pruned = match prune_file {
                    None => true,
                    Some(prune_id) => {
                        match state.prune_data_stored_in_file(prune_id, &mut entry) {
                            Ok(_) => {
                                prune_file = None; // Prune Complete!
                                true
                            },
                            Err(_) => false
                        }
                    }
                };

                let migrated = pruned && state.migrate_earliest_entry(&mut entry);

                if pruned && migrated {
                    // If the last entry wasn't able to process all entries it read from the
                    // channel, it will have left the last entry it read in the next_request field
                    // of the log state object. Handle that before reading new entries
                    if ! state.next_request.is_none() {
                        if let Some(request) = state.next_request.clone() {
                            match state.add_request(&mut entry, &request) {
                                RequestResult::Okay => state.next_request = None,
                                RequestResult::EntryIsFull => ()
                            }
                        }
                    }

                    // If we weren't able to handle the pending request, the entry must already
                    // be full so we can skip reading from the channel. Otherwise, we'll read from
                    // the channel until the entry is full or we need to do a blocking read.
                    if state.next_request.is_none() {

                        'read_loop: loop {

                            match state.receiver.try_recv() {
                                Ok(request) => {
                                    match state.add_request(&mut entry, &request) {
                                        RequestResult::Okay => (),
                                        RequestResult::EntryIsFull => {
                                            state.next_request = Some(request);
                                            break 'read_loop;
                                        }
                                    };
                                },
                                Err(e) => {
                                    match e {
                                        TryRecvError::Disconnected => break 'top_level,

                                        TryRecvError::Empty => {
                                            if ! entry.is_empty() {
                                                // The channel is empty and we have content to
                                                // write
                                                break 'read_loop; 
                                            } else {
                                                // channel and entry are empty
                                                // Block here awaiting entry content. Note that
                                                // we're HOLDING the state mutex while blocking
                                                // that's what we want.
                                                match state.receiver.recv() {
                                                    // The only reason channel.recv() can error
                                                    // here is due the channel being both empty
                                                    // and broken. Terminate this IO thread.
                                                    Err(e) => break 'top_level,

                                                    Ok(request) => {
                                                        match state.add_request(&mut entry, &request) {
                                                            RequestResult::Okay => (),
                                                            RequestResult::EntryIsFull => {
                                                                state.next_request = Some(request);
                                                                break 'read_loop;
                                                            }
                                                        };
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                let (entry_serial, buffers) = state.create_log_entry(&mut entry, &stream);

                let prune_required = ! state.next_request.is_none();

                

                (prune_required, entry_serial, buffers)
            }; // ---------- Unlock Log State Mutex ----------

            // Blocking Write
            stream.write(buffers);

            if prune_required || entry.is_empty() {
                prune_file = stream.rotate_files();
            }
            
            { // ---------- Lock Log State Mutex ----------
                let mut state = self.log_state.lock().unwrap(); // Panic if lock fails

                if entry_serial == state.last_notified_serial.next() {
                    // Run completion handlers for this entry
                    for cr in &entry.requests {
                        match cr {
                            Completion::TxSave(client_id, request_id) => state.completion_handlers[client_id.0].transaction_state_saved(*request_id),
                            Completion::AllocSave(client_id, request_id) => state.completion_handlers[client_id.0].allocation_state_saved(*request_id)
                        }
                    }

                    state.last_notified_serial = entry_serial;

                    // Run completion handlers for further entries that have completed
                    loop {
                        let next = state.last_notified_serial.next();

                        match state.pending_completions.remove(&next) {
                            None => break,
                            Some(v) => {
                                for cr in v {
                                    match cr {
                                        Completion::TxSave(client_id, request_id) => state.completion_handlers[client_id.0].transaction_state_saved(request_id),
                                        Completion::AllocSave(client_id, request_id) => state.completion_handlers[client_id.0].allocation_state_saved(request_id)
                                    }
                                }
                                state.last_notified_serial = next;
                            }
                        }
                    }
                } else {
                    state.pending_completions.insert(entry_serial, entry.requests.clone());
                }
                
            } // ---------- Unlock Log State Mutex ----------
        }
    }
}

#[derive(Copy, Clone)]
enum Completion {
    TxSave(ClientId, RequestId),
    AllocSave(ClientId, RequestId)
}

struct Entry {
    requests: Vec<Completion>,
    tx_set: HashSet<TxId>,
    tx_deletions: Vec<TxId>,
    allocs: Vec<TxId>,
    alloc_deletions: Vec<TxId>,
    max_size: usize,
    size: usize,
    offset: usize
}

impl Entry {
    fn new(max_file_size: usize) -> Entry {

        // Reduce the raw maximum file size by the size of the static entry block
        // and ensure sufficient space for padding the entry to end on a 4k aligned
        // boundary.
        let max_file_size = max_file_size - STATIC_ENTRY_SIZE as usize - 4096*2;

        Entry {
            requests: Vec::new(),
            tx_set: HashSet::new(),
            tx_deletions: Vec::new(),
            allocs: Vec::new(),
            alloc_deletions: Vec::new(),
            max_size: max_file_size,
            size: 0,
            offset: 0
        }
    }

    fn reset(&mut self, stream: &Box<&mut dyn FileStream>) {
        self.requests.clear();
        self.tx_set.clear();
        self.tx_deletions.clear();
        self.allocs.clear();
        self.alloc_deletions.clear();
        self.size = 0;
        let (_, _, file_offset) = stream.status();
        self.offset = file_offset;
    }

    fn is_empty(&self) -> bool {
        self.tx_set.is_empty() &&
        self.tx_deletions.is_empty() && 
        self.allocs.is_empty() &&
        self.alloc_deletions.is_empty()
    }

    fn add_transaction(&mut self, 
        tx_id: &TxId, 
        tx: &RefCell<Tx>,
        req: Option<&ClientRequest>) -> Result<(), EntryFull> {
        if self.tx_set.contains(tx_id) {
            if let Some(cr) = req {
                self.requests.push(Completion::TxSave(cr.0, cr.1));
            }
            return Ok(());
        } else {
            
            let esize = encoding::tx_write_size(tx);
            if self.size + esize > self.max_size {
                return Err(EntryFull{});
            } else {
                self.tx_set.insert(tx_id.clone());
                self.size += esize;
                if let Some(cr) = req {
                    self.requests.push(Completion::TxSave(cr.0, cr.1));
                }
                return Ok(());
            }
        }
    }

    fn add_allocation(&mut self, 
        tx_id: &TxId, 
        alloc: &RefCell<Alloc>,
        req: Option<&ClientRequest>) -> Result<(), EntryFull> {
        let asize = encoding::alloc_write_size(alloc);
        if self.size + asize > self.max_size {
            return Err(EntryFull{});
        } else {
            self.allocs.push(tx_id.clone());
            self.size += asize;
            if let Some(cr) = req {
                self.requests.push(Completion::AllocSave(cr.0, cr.1));
            }
            return Ok(());
        }
    }

    fn drop_transaction(&mut self, tx_id: &TxId) -> Result<(), EntryFull> {
        let tdsize = encoding::tx_delete_size(tx_id);
        if self.size + tdsize > self.max_size {
            return Err(EntryFull{});
        } else {
            self.tx_deletions.push(tx_id.clone());
            self.size += tdsize;
            return Ok(());
        }
    }

    fn drop_allocation(&mut self, tx_id: &TxId) -> Result<(), EntryFull> {
        let adsize = encoding::alloc_delete_size(&tx_id);
        if self.size + adsize > self.max_size {
            return Err(EntryFull{});
        } else {
            self.alloc_deletions.push(tx_id.clone());
            self.size += adsize;
            return Ok(());
        }
    }
}