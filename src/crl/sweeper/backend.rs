
use std::sync::{Mutex, Arc};
use std::thread;

use crossbeam::crossbeam_channel;
use crossbeam::crossbeam_channel::TryRecvError;

use crate::crl::{RequestCompletionHandler, Crl};
use crate::crl;
use crate::object;
use crate::transaction;
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

    completion_handlers: Vec<Arc<dyn RequestCompletionHandler + Send + Sync>>,
    last_notified_serial: LogEntrySerialNumber,

    pending_completions: HashMap<LogEntrySerialNumber, (bool, Vec<Completion>)>,

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
                    serialized_transaction_description: rtx.serialized_transaction_description.clone().into(),
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
                entry.add_allocation(tx_id, a, None)?;
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

                        mtx.state.tx_disposition = *tx_disposition;
                        mtx.state.paxos_state = *paxos_state;

                        // Check for late arrival of object update content
                        if mtx.state.object_updates.len() == 0 && object_updates.len() != 0 {
                            mtx.state.object_updates = object_updates.clone();
                            mtx.data_locations = None;
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
                    // can safely ignore errors here
                    drop(mtx);
                    match entry.add_transaction(&txid, &tx, None) {
                        Ok(_) => RequestResult::Okay,
                        Err(_) => RequestResult::EntryIsFull
                    }
                } else {
                    RequestResult::Okay
                }
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
                    Err(_) => {
                        RequestResult::EntryIsFull
                    }
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
                    Err(_) => {
                        RequestResult::EntryIsFull
                    }
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

                // Intentionally ignore any send errors
                sender.send(FullStateResponse(txs, allocs)).unwrap_or(()); 
                RequestResult::Okay   
            },
            Request::RegisterClientRequest {
                sender,
                handler
            } => {
                let client_id = ClientId(self.completion_handlers.len());
                self.completion_handlers.push(handler.clone());

                // Intentionally ignore any send errors
                sender.send(RegisterClientResponse { client_id }).unwrap_or(()); 
                RequestResult::Okay
            },
            Request::Terminate => RequestResult::Terminate
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
        stream: &Box<dyn FileStream>) -> (LogEntrySerialNumber, Vec::<ArcDataSlice>) {
        
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
    EntryIsFull,
    Terminate
}

pub(super) struct Backend {
    io_threads: Vec<thread::JoinHandle<()>>,
    pub sender: crossbeam_channel::Sender<Request>
}

impl crate::crl::Backend for Backend {
    fn shutdown(&mut self) {
        self.shutdown_impl();
    }

    fn new_interface(&self, 
        save_handler: sync::Arc<dyn RequestCompletionHandler + Send + Sync>) -> Box<dyn Crl> {

        let (response_sender, receiver) = crossbeam_channel::unbounded();

        self.sender.send(Request::RegisterClientRequest{sender: response_sender, handler: save_handler}).unwrap();

        let client_id = receiver.recv().unwrap().client_id;

        Box::new(frontend::Frontend::new(client_id, self.sender.clone()))
    }
}

impl Backend {

    pub fn shutdown_impl(&mut self) {

        for _ in &self.io_threads {
            // Intentionally ignore send errors
            self.sender.send(Request::Terminate).unwrap_or(());
        }
        while !self.io_threads.is_empty() {
            self.io_threads.pop().map(|t| t.join());
        }
    }

    pub fn recover(
        crl_directory: &Path, 
        entry_window_size: usize,
        max_entry_operations: usize,
        num_streams: usize,
        max_file_size: usize
        ) -> Result<Backend, std::io::Error> {

        let mut r = log_file::recover(crl_directory, max_file_size, num_streams)?;
        
        assert!(r.log_files.len() % 3 == 0);

        let (sender, receiver) = crossbeam_channel::unbounded();
        
        let log_state = LogState::new(
                    receiver, entry_window_size, 
                    &r.transactions, &r.allocations, 
                    r.last_entry_serial, r.last_entry_location);

        let mut io_threads = Vec::new();

        while r.log_files.len() > 0 {

            let f3 = r.log_files.pop().unwrap();
            let f2 = r.log_files.pop().unwrap();
            let f1 = r.log_files.pop().unwrap();

            let ls = log_state.clone();

            let handle = thread::spawn( move || {
                io_thread(ls, tri_file_stream::TriFileStream::new(f1, f2, f3), max_entry_operations);
            });

            io_threads.push(handle);
        }

        let be = Backend{
            io_threads,
            sender,
        };

        Ok(be)
    }
}

fn io_thread(log_state: Arc<Mutex<LogState>>, mut stream: Box<dyn FileStream>, max_entry_operations: usize) {

    let max_file_size = stream.const_max_file_size();

    let mut prune_file: Option<FileId> = None;
    let mut entry = Entry::new(max_file_size, max_entry_operations);

    'top_level: loop {
        
        entry.reset(&stream);

        let (prune_required, entry_serial, buffers) = { 
            // ---------- Lock Log State Mutex ----------

            let mut state = log_state.lock().unwrap(); // Panic if lock fails

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
                            RequestResult::EntryIsFull => (),
                            RequestResult::Terminate => break 'top_level
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
                                    },
                                    RequestResult::Terminate => break 'top_level
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
                                                Err(_) => break 'top_level,

                                                Ok(request) => {
                                                    match state.add_request(&mut entry, &request) {
                                                        RequestResult::Okay => (),
                                                        RequestResult::EntryIsFull => {
                                                            state.next_request = Some(request);
                                                            break 'read_loop;
                                                        },
                                                        RequestResult::Terminate => {
                                                            break 'top_level;
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
        let success = match stream.write(buffers) {
            Ok(_) => true,
            Err(_) => {
                // TODO - Log error
                false
            }
        };

        if prune_required || entry.is_empty() {
            match stream.rotate_files() {
                Ok(fid) => prune_file = fid,
                Err(_) => {
                    // TODO: Log error and exit thread
                    // This stream is busted. Exit thread
                    break 'top_level
                } 
            }
        }
        
        { // ---------- Lock Log State Mutex ----------
            let mut state = log_state.lock().unwrap(); // Panic if lock fails

            if entry_serial == state.last_notified_serial.next() {
                // Run completion handlers for this entry
                for cr in &entry.requests {
                    match cr {
                        Completion::TxSave {
                            client_id, 
                            store_id, 
                            transaction_id, 
                            save_id 
                        } => {
                            state.completion_handlers[client_id.0].complete(
                                crl::Completion::TransactionSave {
                                    store_id: *store_id,
                                    transaction_id: *transaction_id,
                                    save_id: *save_id, 
                                    success
                                }
                            )
                        },
                        Completion::AllocSave {
                            client_id, 
                            store_id, 
                            object_id 
                        } => {
                            state.completion_handlers[client_id.0].complete(
                                crl::Completion::AllocationSave {
                                    store_id: *store_id,
                                    object_id: *object_id, 
                                    success
                                }
                            )
                        }
                    }
                }

                state.last_notified_serial = entry_serial;

                // Run completion handlers for further entries that have completed
                loop {
                    let next = state.last_notified_serial.next();

                    match state.pending_completions.remove(&next) {
                        None => break,
                        Some(t) => {
                            let (psuccess, v) = t;
                            for cr in v {
                                match cr {
                                    Completion::TxSave {
                                        client_id, 
                                        store_id, 
                                        transaction_id, 
                                        save_id
                                    } => {
                                        state.completion_handlers[client_id.0].complete(
                                            crl::Completion::TransactionSave {
                                                store_id,
                                                transaction_id,
                                                save_id, 
                                                success: psuccess
                                            }
                                        )
                                    },
                                    Completion::AllocSave {
                                        client_id, 
                                        store_id, 
                                        object_id 
                                    } => {
                                        state.completion_handlers[client_id.0].complete(
                                            crl::Completion::AllocationSave {
                                                store_id,
                                                object_id, 
                                                success: psuccess
                                            }
                                        )
                                    }
                                }
                            }
                            state.last_notified_serial = next;
                        }
                    }
                }
            } else {
                state.pending_completions.insert(entry_serial, (success, entry.requests.clone()));
            }
            
        } // ---------- Unlock Log State Mutex ----------
    }
}

#[derive(Copy, Clone)]
enum Completion {
    TxSave {
        client_id: ClientId, 
        store_id: store::Id, 
        transaction_id: transaction::Id, 
        save_id: TxSaveId 
    },
    AllocSave { 
        client_id: ClientId, 
        store_id: store::Id, 
        object_id: object::Id
    }
}

struct Entry {
    requests: Vec<Completion>,
    tx_set: HashSet<TxId>,
    tx_deletions: Vec<TxId>,
    allocs: Vec<TxId>,
    alloc_deletions: Vec<TxId>,
    max_size: usize,
    max_operations: usize,
    size: usize,
    offset: usize,
    operations: usize
}

impl Entry {
    fn new(max_file_size: usize, max_operations: usize) -> Entry {

        Entry {
            requests: Vec::new(),
            tx_set: HashSet::new(),
            tx_deletions: Vec::new(),
            allocs: Vec::new(),
            alloc_deletions: Vec::new(),
            max_size: max_file_size,
            max_operations,
            size: 0,
            offset: 0,
            operations: 0
        }
    }

    fn reset(&mut self, stream: &Box<dyn FileStream>) {
        self.requests.clear();
        self.tx_set.clear();
        self.tx_deletions.clear();
        self.allocs.clear();
        self.alloc_deletions.clear();
        self.size = 0;
        let (_, _, file_offset) = stream.status();
        self.offset = file_offset;
        self.operations = 0;
    }

    fn is_empty(&self) -> bool {
        self.tx_set.is_empty() &&
        self.tx_deletions.is_empty() && 
        self.allocs.is_empty() &&
        self.alloc_deletions.is_empty()
    }

    fn have_room_for(&self, nbytes: usize) -> bool {
        self.offset + self.size + nbytes + STATIC_ENTRY_SIZE as usize + 4096 <= self.max_size
    }

    fn add_transaction(&mut self, 
        tx_id: &TxId, 
        tx: &RefCell<Tx>,
        req: Option<&ClientRequest>) -> Result<(), EntryFull> {
            
        if self.operations == self.max_operations {
            return Err(EntryFull{});
        }

        if self.tx_set.contains(tx_id) {
            // Transaction is already part of this entry
            if let Some(cr) = req {
                self.requests.push(Completion::TxSave {
                    client_id: cr.client_id, 
                    store_id: cr.store_id, 
                    transaction_id: cr.transaction_id, 
                    save_id: cr.save_id
                });
            }
            return Ok(());
        } else {
            
            let esize = encoding::tx_write_size(tx);

            if self.have_room_for(esize) {
                self.operations += 1;
                self.tx_set.insert(tx_id.clone());
                self.size += esize;
                if let Some(cr) = req {
                    self.requests.push(Completion::TxSave{
                        client_id: cr.client_id, 
                        store_id: cr.store_id, 
                        transaction_id: cr.transaction_id, 
                        save_id: cr.save_id
                    });
                }
                return Ok(());
            } else {
                return Err(EntryFull{});   
            }
        }
    }

    fn add_allocation(&mut self, 
        tx_id: &TxId, 
        alloc: &RefCell<Alloc>,
        req: Option<&ClientRequest>) -> Result<(), EntryFull> {

        if self.operations == self.max_operations {
            return Err(EntryFull{});
        }

        let object_id = alloc.borrow().state.id;

        let asize = encoding::alloc_write_size(alloc);

        if self.have_room_for(asize) {
            self.operations += 1;
            self.allocs.push(tx_id.clone());
            self.size += asize;
            if let Some(cr) = req {
                self.requests.push(Completion::AllocSave {
                    client_id: cr.client_id, 
                    store_id: cr.store_id, 
                    object_id
                });
            }
            return Ok(());
        } else {
            return Err(EntryFull{});
        }
    }

    fn drop_transaction(&mut self, tx_id: &TxId) -> Result<(), EntryFull> {

        if self.operations == self.max_operations {
            return Err(EntryFull{});
        }

        let tdsize = encoding::tx_delete_size(tx_id);

        if self.have_room_for(tdsize) {
            self.operations += 1;
            self.tx_deletions.push(tx_id.clone());
            self.size += tdsize;
            return Ok(());
        } else {
            return Err(EntryFull{});
        }
    }

    fn drop_allocation(&mut self, tx_id: &TxId) -> Result<(), EntryFull> {

        if self.operations == self.max_operations {
            return Err(EntryFull{});
        }

        let adsize = encoding::alloc_delete_size(&tx_id);

        if self.have_room_for(adsize) {
            self.operations += 1;
            self.alloc_deletions.push(tx_id.clone());
            self.size += adsize;
            return Ok(());
        } else {
            return Err(EntryFull{});
        }
    }
}