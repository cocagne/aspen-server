
use super::*;
use log_file::LogFile;

#[derive(Clone, Copy)]
enum Index {
    Zero = 0,
    One = 1,
    Two = 2
}

pub(super) struct TriFileStream {
    files: [LogFile; 3],
    active: Index,
}

impl TriFileStream {
    pub fn new(
        f1: (LogFile, Option<LogEntrySerialNumber>),
        f2: (LogFile, Option<LogEntrySerialNumber>),
        f3: (LogFile, Option<LogEntrySerialNumber>)) -> Box<dyn FileStream> {

        let mut active = Index::Zero;
        let mut highest = match f1.1 {
            None => LogEntrySerialNumber(0),
            Some(sn) => sn
        };

        if let Some(sn) = f2.1 {
            if sn > highest {
                active = Index::One;
                highest = sn;
            }
        } 

        if let Some(sn) = f3.1 {
            if sn > highest {
                active = Index::Two;
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
        self.files[self.active as usize].max_size
    }

    fn status(&self) -> (FileId, uuid::Uuid, usize) {
        let c = &self.files[self.active as usize];
        (c.file_id, c.file_uuid, c.len)
    }

    fn write(&mut self, data: Vec<ArcDataSlice>) {
        self.files[self.active as usize].write(&data);
    }
    
    fn rotate_files(&mut self) -> Option<FileId> {

        let (new_active, retire) = match self.active {
            Index::Zero => (Index::One,  Index::Two),
            Index::One  => (Index::Two,  Index::Zero),
            Index::Two  => (Index::Zero, Index::One)
        };

        self.active = new_active;
        self.files[self.active as usize].recycle();

        return Some(self.files[retire as usize].file_id);
    }
}
