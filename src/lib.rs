extern crate time;
extern crate uuid;

pub mod crl;
pub mod hlc;
pub mod object;
pub mod paxos;
pub mod store;
pub mod transaction;
pub mod util;

pub enum EncodingError {
    ValueOutOfRange
}