
use super::*;
use log_file::LogFile;


pub(super) struct TriFileStream {
    files: [LogFile; 3],
    active: usize,
}

impl TriFileStream {
    pub fn new(
        f1: (LogFile, Option<LogEntrySerialNumber>),
        f2: (LogFile, Option<LogEntrySerialNumber>),
        f3: (LogFile, Option<LogEntrySerialNumber>)) -> Box<dyn FileStream> {

        let mut active = 0;
        let highest = match f1.1 {
            None => LogEntrySerialNumber(0),
            Some(sn) => sn
        };

        if let Some(sn) = f2.1 {
            if sn > highest {
                active = 1;
                highest = sn;
            }
        } 

        if let Some(sn) = f3.1 {
            if sn > highest {
                active = 2;
                highest = sn;
            }
        }

        Box::new(TriFileStream {
            files: [f1.0, f2.0, f3.0],
            active
        })
    }
}

impl FileStream for TriFileStream {
    fn const_max_file_size(&self) -> usize {
        self.files[self.active].max_size
    }

    fn status(&self) -> (FileId, uuid::Uuid, usize) {
        let c = &self.files[self.active];
        (c.file_id, c.file_uuid, c.len)
    }

    fn write(&mut self, data: Vec<ArcDataSlice>) {
        self.files[self.active].write(&data);
    }
    
    fn rotate_files(&mut self) -> Option<FileId> {

        let (new_active, retire) = match self.active {
            0 => (1, 2),
            1 => (2, 0),
            2 => (0, 1)
        };

        self.active = new_active;
        self.files[self.active].recycle();

        return Some(self.files[retire].file_id);
    }
}
