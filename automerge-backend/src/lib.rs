#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::similar_names)]
#![allow(clippy::shadow_unrelated)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::redundant_pub_crate)]
#![allow(clippy::missing_const_for_fn)]
#![allow(clippy::use_self)]
#![allow(clippy::too_many_lines)]

extern crate fxhash;
extern crate hex;
extern crate itertools;
extern crate maplit;
extern crate rand;
extern crate web_sys;

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
pub use sync::{BloomFilter, SyncHave, SyncMessage, SyncState};

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
