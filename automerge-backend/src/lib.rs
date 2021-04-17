extern crate fxhash;
extern crate hex;
extern crate itertools;
extern crate maplit;
extern crate rand;
extern crate web_sys;

#[allow(unused_macros)]
macro_rules! log {
    ( $( $t:tt )* ) => {
        web_sys::console::log_1(&format!( $( $t )* ).into());
    }
}

mod actor_map;
mod backend;
mod change;
mod columnar;
mod concurrent_operations;
mod encoding;
mod error;
mod internal;
mod object_store;
mod op_handle;
mod op_set;
mod ordered_set;
mod pending_diff;
mod sync;

pub use backend::Backend;
pub use change::Change;
pub use error::AutomergeError;
pub use sync::BloomFilter;
pub use sync::SyncHave;
pub use sync::SyncMessage;
pub use sync::SyncState;

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        thread,
    };

    #[test]
    fn sync_and_send_backend() {
        let b = crate::Backend::init();
        let mb = Arc::new(Mutex::new(b));
        thread::spawn(move || {
            let b = mb.lock().unwrap();
            b.get_changes(&[]);
        });
    }
}
