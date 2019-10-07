///! Single-threaded manager for multiple data stores
///!
///! Provides channel-based linkage between the network threads, backend I/O threads and
///! the data stores they communicate with.
///!
/// 

use crossbeam::channel;

use crate::store::backend;

enum Messages {
    IOCompletion {
        op: backend::Completion;
    }
}

struct StoreManager {

}