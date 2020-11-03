extern crate jemallocator;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

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
pub mod encoding;
pub mod finalizer;

pub use data::{ArcData, ArcDataSlice, RawData, Data, DataMut, DataReader, SliceReader};

pub enum EncodingError {
    ValueOutOfRange
}