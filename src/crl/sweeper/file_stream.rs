
use super::FileId;
use crate::ArcDataSlice;

/// Abstract interface to a multi-file log stream
pub(super) trait FileStream {

    // Constant Maximum File Size
    fn const_max_file_size(&self) -> usize;

    /// Returns the current file Id, file UUID, Offset
    fn status(&self) -> (FileId, uuid::Uuid, usize);

    /// Writes the provided data to the file provided by status()
    /// 
    /// Returns nothing on success and and error if the write failed
    fn write(&mut self, data: Vec<ArcDataSlice>); 

    /// Rotates the underlying files and optionally returns a FileId
    /// to prune entries from
    fn rotate_files(&mut self) -> Option<FileId>;
}