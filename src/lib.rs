extern crate time;
extern crate uuid;

pub mod crl;
pub mod data;
pub mod hlc;
pub mod ida;
pub mod network;
pub mod object;
pub mod paxos;
pub mod pool;
pub mod store;
pub mod transaction;
pub mod util;

pub use data::{ArcData, ArcDataSlice, Data, DataMut, DataReader, SliceReader};

pub enum EncodingError {
    ValueOutOfRange
}