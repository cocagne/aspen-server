use std::ffi::CString;
use std::io;
use std::io::ErrorKind;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use libc;

use super::*;

struct FChannel {
    file_id: FileId,
    file_uuid: [u8; 16],
    fd: libc::c_int,
    offset: usize,
    last: Option<(LogEntrySerialNumber, FileLocation)>
}

struct DioGather {
    files: [FChannel; 3],
    max_file_size: usize,
    active_channel: usize,
    last: Option<(LogEntrySerialNumber, FileLocation)>
}


impl FChannel {
    fn new(file_id: FileId, directory: &Path) -> Result<FChannel, io::Error> {
        let f = format!("{}", file_id.0);
        let p = directory.join(f);
        let fp = p.as_path();

        let fd = unsafe {
            let cpath = CString::new(fp.as_os_str().as_bytes()).unwrap();
            libc::open(cpath.as_ptr(), libc::O_CREAT | libc::O_RDWR)
        };

        if fd < 0 {
            return Err(io::Error::new(ErrorKind::Other, "Failed to open file"));
        }

        let mut size = unsafe {
            let sz = libc::lseek(fd, 0, libc::SEEK_END);
            libc::lseek(fd, 0, libc::SEEK_SET);
            sz
        };

        if size < (16 + STATIC_ENTRY_SIZE as i64) {
            // Initialize 
            let fuuid = uuid::Uuid::new_v4().as_bytes();
            unsafe {
                let u: *const u8 = &fuuid[0];
                libc::write(fd,u as *const libc::c_void, 16);
                libc::fsync(fd);
                libc::lseek(fd, 0, libc::SEEK_SET);
            }
            size = 16;
        }

        let mut file_uuid: [u8; 16] = [0; 16];
        let puuid: *mut u8 = &mut file_uuid[0];

        unsafe {
            libc::read(fd, puuid as *mut libc::c_void, 16);
        }

        let last = find_last_valid_entry(fd, file_uuid, size);

        Ok(FChannel{
            file_id,
            file_uuid,
            fd,
            offset: size as usize,
            last
        })
   }

   fn write(&mut self, data: &Vec<ArcDataSlice>, entry_serial_number: &LogEntrySerialNumber) -> FileLocation {
        FileLocation {
            file_id: FileId(0),
            offset: 0,
            length: 0
        }
   }
}

fn find_last_valid_entry(
    fd: libc::c_int,
    file_uuid: [u8; 16],
    file_size: libc::c_long) -> Option<(LogEntrySerialNumber, FileLocation)> {
    
    let mut offset = file_size - file_size % 4096;
    let mut test_uuid: [u8; 16] = [0; 16];
    let mut found = false;

    let tuuid: *mut u8 = &mut test_uuid[0];

    while offset > 32 && ! found {
        unsafe {
            libc::lseek(fd, offset - 16, libc::SEEK_SET);
            libc::read(fd, tuuid as *mut libc::c_void, 16);
        }
        if test_uuid == file_uuid {
            found = true;
            libc::lseek(fd, offset, libc::SEEK_SET);
            break;
        }
        offset -= 4096;
    }

    if found {
        unsafe {

        }
    } else {
        None
    }
}



impl DioGather {
    pub fn new(
        directory: &Path,
        base_file_id: FileId, 
        max_file_size: usize) -> Result<Box<dyn FileStream>, io::Error> {

        if ! directory.is_dir() {
            return Err(io::Error::new(ErrorKind::InvalidInput, "Not a directory"));
        }

        let files = [
            FChannel::new(FileId(base_file_id.0 + 0), directory)?,
            FChannel::new(FileId(base_file_id.0 + 1), directory)?,
            FChannel::new(FileId(base_file_id.0 + 2), directory)?,
        ];


        let mut last: Option<(usize, LogEntrySerialNumber, FileLocation)> = None; 
        
        for (i, f) in files.iter().enumerate() {
            match (last, f.last) {
                (None, None) => (),
                (Some(t), None) => (),
                (None, Some((serial, loc))) => last = Some((i, serial, loc)),
                (Some(t), Some(l)) => {
                    if l.0 > t.1 {
                        last = Some((i, l.0, l.1))
                    }
                }
            }
        }

        let (active_channel, last) = last.map(|x| (x.0, Some((x.1, x.2)))).unwrap_or((0, None));

        Ok(Box::new(DioGather {
            files,
            max_file_size,
            active_channel,
            last
        }))
    }
}

impl FileStream for DioGather {
    fn const_max_file_size(&self) -> usize {
        self.max_file_size
    }

    fn status(&self) -> (FileId, uuid::Uuid, usize) {
        let c = &self.files[self.active_channel];
        (c.file_id, c.file_uuid, c.offset)
    }

    fn write(&mut self, data: Vec<ArcDataSlice>, entry_serial_number: LogEntrySerialNumber) {
        let loc = self.files[self.active_channel].write(&data, &entry_serial_number);
        self.last = Some((entry_serial_number, loc));
    }
    
    fn rotate_files(&mut self) -> Option<FileId> {
        if self.active_channel == 0 {
            self.active_channel = 1;
            return Some(self.files[2].file_id);
        }
        
        if self.active_channel == 1 {
            self.active_channel = 2;
            return Some(self.files[0].file_id);
        }
       
        self.active_channel = 0;
        return Some(self.files[1].file_id);
    }
}
