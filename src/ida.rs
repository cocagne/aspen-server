#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum IDA {
    Replication {
        width: u8,
        write_threshold: u8
    }
}