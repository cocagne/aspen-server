#[derive(Debug, Copy, Clone)]
pub enum IDA {
    Replication {
        width: u8,
        write_threshold: u8
    }
}