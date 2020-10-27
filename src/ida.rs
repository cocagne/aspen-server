#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum IDA {
    Replication {
        width: u8,
        write_threshold: u8
    },
    ReedSolomon {
        width: u8,
        read_threshold: u8,
        write_threshold: u8
    }
}

impl IDA {
    pub fn width(&self) -> u8 {
        match self {
            IDA::Replication{ width, write_threshold } => *width,
            IDA::ReedSolomon{ width, write_threshold, read_threshold } => *width
        }
    }

    pub fn write_threshold(&self) -> u8 {
        match self {
            IDA::Replication{ width, write_threshold } => *write_threshold,
            IDA::ReedSolomon{ width, write_threshold, read_threshold } => *write_threshold
        }
    }

    pub fn read_threshold(&self) -> u8 {
        match self {
            IDA::Replication{ width, write_threshold } => *width / 2 + 1,
            IDA::ReedSolomon{ width, write_threshold, read_threshold } => *read_threshold
        }
    }
}