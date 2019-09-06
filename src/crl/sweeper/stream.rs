
use super::{FileLocation, FileId};
use crate::ArcDataSlice;
use crate::crl::LogEntrySerialNumber;

/// Wrapper for numericly identified stream
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, Hash)]
pub struct StreamId(usize);

/// Abstract interface to a multi-file log stream
pub trait Stream {

    /// Assigns the ID for this stream
    fn set_id(&self, id: StreamId);

    /// Returns true iff this stream is ready for writing
    fn is_ready(&self) -> bool;

    /// Returns the last valid entry in this stream
    fn last_entry(&self) -> Option<(LogEntrySerialNumber, FileLocation)>;

    /// Returns the current file Id, file UUID, Offset, & Maximum File Size
    fn status(&self) -> (FileId, uuid::Uuid, u64, u64);

    /// Writes the provided data to the file provided by status()
    /// 
    /// Currently this method must always be able to write data of the maximum-possible size.
    fn write(&self, data: Vec<ArcDataSlice>, entry_serial_number: LogEntrySerialNumber);

    /// Rotates the underlying files and optionally returns a FileId
    /// to prune entries from
    fn rotate_files(&self) -> Option<FileId>;
}
