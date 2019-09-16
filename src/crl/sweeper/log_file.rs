use std::ffi::CString;
use std::io::{Result, Error};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use libc;

use super::{FileId, STATIC_ENTRY_SIZE, LogEntrySerialNumber};
use crate::{Data, ArcDataSlice};

struct LogFile {
    pub file_id: FileId,
    fd: libc::c_int,
    offset: usize,
    size: usize,
    pub file_uuid: uuid::Uuid
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



impl LogFile {
    fn new(directory: &Path, file_id: FileId) -> Result<(LogFile, Option<(LogEntrySerialNumber, usize)>)> {
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
            offset: size as usize,
            size: size as usize,
            file_uuid
        };

        Ok((lf, last))
    }

    fn write(&mut self, data: &Vec<ArcDataSlice>) {
        let iov: Vec<libc::iovec> = data.iter().map( |d| {
            let p: *mut u8 = &mut d.as_bytes()[0];
            libc::iovec {
                iov_base: p as *mut libc::c_void,
                iov_len: d.len()
            }
        }).collect();
        unsafe {
            libc::writev(self.fd, &iov[0], data.len() as libc::c_int);
        }
    }

    fn read(&self, offset: usize, nbytes: usize) -> Data {
        let mut v = Vec::<u8>::with_capacity(nbytes);
        v.resize(nbytes, 0);
        pread_bytes(self.fd, &mut v[..], offset);
        Data::new(v)
    }
}

