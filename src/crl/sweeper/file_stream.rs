
use super::{FileId, LogEntrySerialNumber};
use crate::ArcDataSlice;

/// Abstract interface to a multi-file log stream
pub(super) trait FileStream {

    /// Returns the last valid entry in this stream
    //fn last_entry(&self) -> Option<(LogEntrySerialNumber, FileLocation)>;

    //fn all_files(&self) -> &[FileId];

    // Constant Maximum File Size
    fn const_max_file_size(&self) -> usize;

    /// Returns the current file Id, file UUID, Offset
    fn status(&self) -> (FileId, uuid::Uuid, usize);

    /// Writes the provided data to the file provided by status()
    /// 
    /// Currently this method must always be able to write data of the maximum-possible size.
    fn write(&self, data: Vec<ArcDataSlice>, entry_serial_number: LogEntrySerialNumber);

    /// Rotates the underlying files and optionally returns a FileId
    /// to prune entries from
    fn rotate_files(&self) -> Option<FileId>;
}
