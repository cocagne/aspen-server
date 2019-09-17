use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::io::{Result, Error, ErrorKind};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use libc;

use crate::{Data, ArcDataSlice};

use super::*;

pub(super) struct LogFile {
    pub file_id: FileId,
    fd: libc::c_int,
    pub len: usize,
    pub max_size: usize,
    pub file_uuid: uuid::Uuid
}

impl LogFile {
    fn new(
        directory: &Path, 
        file_id: FileId,
        max_file_size: usize) -> Result<(LogFile, Option<(LogEntrySerialNumber, usize)>)> {

        let f = format!("{}", file_id.0);
        let p = directory.join(f);
        let fp = p.as_path();

        let fd = unsafe {
            let cpath = CString::new(fp.as_os_str().as_bytes()).unwrap();
            libc::open(cpath.as_ptr(), libc::O_CREAT | libc::O_RDWR)
        };

        if fd < 0 {
            return Err(Error::last_os_error());
        }

        let mut size = seek(fd, 0, libc::SEEK_END)?;
            
        
        if size < (16 + STATIC_ENTRY_SIZE as usize) {
            // Initialize 
            seek(fd, 0, libc::SEEK_SET)?;
            unsafe {
                libc::ftruncate(fd, 0);
            }
            let fuuid = uuid::Uuid::new_v4().as_bytes();
            write_bytes(fd, &fuuid[..])?;
        }

        let file_uuid = pread_uuid(fd, 0)?;

        size = seek(fd, 0, libc::SEEK_END)?;

        let last = find_last_valid_entry(fd, file_id, size, &file_uuid)?;

        let mut lf = LogFile{
            file_id,
            fd,
            len: size as usize,
            max_size: max_file_size,
            file_uuid
        };

        Ok((lf, last))
    }

    fn read(&self, offset: usize, nbytes: usize) -> Result<Data> {
        let mut v = Vec::<u8>::with_capacity(nbytes);
        v.resize(nbytes, 0);
        pread_bytes(self.fd, &mut v[..], offset)?;
        Ok(Data::new(v))
    }

    pub(super) fn write(&mut self, data: &Vec<ArcDataSlice>) -> Result<()> {
        let wsize: usize = data.iter().map(|d| d.len()).sum();
        let iov: Vec<libc::iovec> = data.iter().map( |d| {
            let p: *mut u8 = &mut d.as_bytes()[0];
            libc::iovec {
                iov_base: p as *mut libc::c_void,
                iov_len: d.len()
            }
        }).collect();
        unsafe {
            loop {
                if libc::writev(self.fd, &iov[0], data.len() as libc::c_int) >= 0 {
                    break;
                } else {
                    let err = Error::last_os_error();
                    match err.kind() {
                        ErrorKind::Interrupted => (),
                        _ => return Err(err)
                    }
                }
            }
        }
        self.len += wsize;
        Ok(())
    }

    pub(super) fn recycle(&mut self) -> Result<()> {
        seek(self.fd, 0, libc::SEEK_SET)?;

        unsafe {
            libc::ftruncate(self.fd, 0);
        }

        self.file_uuid = uuid::Uuid::new_v4();
        self.len = 16;

        write_bytes(self.fd, &self.file_uuid.as_bytes()[..])?;

        Ok(())
    }
}

fn pread_bytes(fd: libc::c_int, s: &mut [u8], offset: usize) -> Result<()> {
    let p: *mut u8 = &mut s[0];
    unsafe {
        if libc::pread(fd, p as *mut libc::c_void, s.len(), offset as libc::off_t) < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

fn pread_uuid(fd: libc::c_int, offset: usize) -> Result<uuid::Uuid> {
    let mut buf: [u8; 16] = [0; 16];
    pread_bytes(fd, &mut buf[..], offset)?;
    Ok(uuid::Uuid::from_bytes(buf))
}

fn write_bytes(fd: libc::c_int, s: &[u8]) -> Result<()> {
    let p: *const u8 = &s[0];
    unsafe {
        if libc::write(fd, p as *const libc::c_void, s.len()) < 0 {
            return Err(Error::last_os_error());
        }
        libc::fsync(fd);
    }
    Ok(())
}

fn seek(fd: libc::c_int, offset: i64, whence: libc::c_int) -> Result<usize> {
    unsafe {
        let sz = libc::lseek(fd, offset, whence);
        if sz < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(sz as usize)
        }
    }
}

fn fdtell(fd: libc::c_int) -> Result<usize> {
    seek(fd, 0, libc::SEEK_CUR)
}

fn find_last_valid_entry(
    fd: libc::c_int, 
    file_id: FileId, 
    file_size: usize, 
    file_uuid: &uuid::Uuid) -> Result<Option<(LogEntrySerialNumber, usize)>> {
        
    let mut offset = file_size - (file_size % 4096);
    let mut last = None;

    while offset > 32 && last.is_none() {
        
        let test_uuid = pread_uuid(fd, offset - 16)?;
        
        if test_uuid == *file_uuid {
            let entry_offset = offset - STATIC_ENTRY_SIZE as usize;

            let serial_bytes: [u8; 8] = [0; 8];
            
            pread_bytes(fd, &mut serial_bytes[..], entry_offset)?;

            let serial = u64::from_le_bytes(serial_bytes);

            last = Some((LogEntrySerialNumber(serial), entry_offset));
            break;
        }

        offset -= 4096;
    }
    
    Ok(last)
}

pub(super) fn recover(
    crl_directory: &Path, 
    max_file_size: usize) -> Result<RecoveredCrlState> {

    let raw_files = Vec::<(LogFile, Option<(LogEntrySerialNumber, usize)>)>::new();

    let have_file = |i| -> bool {
        let p = crl_directory.join(format!("{}", i));
        p.as_path().exists()
    };

    let mut i = 0;
    while have_file(i) {
        let f = LogFile::new(crl_directory, FileId(i), max_file_size)?;
        raw_files.push(f);
        i += 1;
    }

    let mut last: Option<(LogFile, LogEntrySerialNumber, usize)> = None;

    for t in raw_files {
        if let Some((serial, offset)) = t.1 {
            if let Some((file, cur_serial, _)) = last {
                if serial > cur_serial {
                    last = Some((t.0, serial, offset));
                }
            } else {
                last = Some((t.0, serial, offset))
            }
        }
    }

    let files: Vec<LogFile> = raw_files.iter().map(|t| t.0).collect();

    let crl_files: Vec<(LogFile, Option<LogEntrySerialNumber>)> = raw_files.iter().map(|t| (t.0, t.1.map(|x| x.0))).collect();
    let mut tx: Vec<RecoveredTx> = Vec::new();
    let mut alloc: Vec<RecoveredAlloc> = Vec::new();
    let mut last_entry_serial = LogEntrySerialNumber(0);
    let mut last_entry_location = FileLocation {
        file_id: FileId(0),
        offset: 0,
        length: 0
    };

    if let Some((last_file, last_serial, last_offset)) = last {

        last_entry_serial = last_serial;

        last_entry_location = FileLocation {
            file_id: last_file.file_id,
            offset: last_offset as u64,
            length: STATIC_ENTRY_SIZE as u32
        };

        let transactions: HashMap<TxId, RecoveringTx> = HashMap::new();
        let allocations: HashMap<TxId, RecoveringAlloc> = HashMap::new();
        let deleted_tx: HashSet<TxId> = HashSet::new();
        let deleted_alloc: HashSet<TxId> = HashSet::new();

        let mut file_id = last_file.file_id;
        let mut entry_serial = last_serial;
        let mut entry_offset = last_offset;

        let earliest_serial_needed = {
            let mut d = last_file.read(last_offset, STATIC_ENTRY_SIZE as usize)?;
            let entry = encoding::decode_entry(&mut d)?;
            LogEntrySerialNumber(entry.earliest_needed)
        };

        while entry_serial <= earliest_serial_needed && entry_serial != LogEntrySerialNumber(0) {
            let file = files[file_id.0 as usize];

            let mut d = file.read(entry_offset, STATIC_ENTRY_SIZE as usize)?;
            let mut entry = encoding::decode_entry(&mut d)?;

            entry_serial = entry.serial;

            let entry_data_size = entry.entry_offset as usize - entry_offset;
            let entry_data_start = entry.entry_offset as usize;
            
            let mut entry_data = file.read(entry_data_start, entry_data_size)?;

            encoding::load_entry_data(&mut entry_data, &mut entry, entry_serial);

            for txid in &entry.tx_deletions {
                deleted_tx.insert(*txid);
            }

            for txid in &entry.alloc_deletions {
                deleted_alloc.insert(*txid);
            }

            for rtx in entry.transactions {
                if ! deleted_tx.contains(&rtx.id) && ! transactions.contains_key(&rtx.id) {
                    transactions.insert(rtx.id, rtx);
                }
            }

            for ra in entry.allocations {
                if ! deleted_alloc.contains(&ra.id) && ! allocations.contains_key(&ra.id) {
                    allocations.insert(ra.id, ra);
                }
            }
        }

        let get_data = |file_location: &FileLocation| -> Result<ArcData> {
            let d = files[file_location.file_id.0 as usize].read(file_location.offset as usize, file_location.length as usize)?;
            Ok(d.into())
        };

        let get_slice = |file_location: &FileLocation| -> Result<ArcDataSlice> {
            let d = get_data(file_location)?;
            Ok(d.into())
        };

        for (txid, rtx) in transactions {

            let ou: Vec<transaction::ObjectUpdate> = Vec::with_capacity(rtx.object_updates.len());

            for t in &rtx.object_updates {
                ou.push(transaction::ObjectUpdate {
                    object_id: object::Id(t.0),
                    data: get_slice(&t.1)?
                });
            }

            tx.push( RecoveredTx {
                id: txid,
                txd_location: rtx.serialized_transaction_description,
                serialized_transaction_description: get_data(&rtx.serialized_transaction_description)?,
                object_updates: ou,
                update_locations: rtx.object_updates,
                tx_disposition: rtx.tx_disposition,
                paxos_state: rtx.paxos_state,
                last_entry_serial: rtx.last_entry_serial
            });
        }

        for (txid, ra) in allocations {

            alloc.push(RecoveredAlloc{
                id: txid,
                store_pointer: ra.store_pointer,
                object_id: ra.object_id,
                kind: ra.kind,
                size: ra.size,
                data_location: ra.data,
                data: get_data(&ra.data)?,
                refcount: ra.refcount,
                timestamp: ra.timestamp,
                serialized_revision_guard: ra.serialized_revision_guard,
                last_entry_serial: ra.last_entry_serial
            });
        }
    };

    Ok(RecoveredCrlState {
        log_files: crl_files,
        transactions: tx,
        allocations: alloc,
        last_entry_serial,
        last_entry_location
    })
}


