extern crate hex;
extern crate im_rc;
extern crate maplit;
extern crate rand;
extern crate uuid;
extern crate web_sys;
extern crate fxhash;

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
mod op;
mod op_handle;
mod op_set;
mod op_type;
mod ordered_set;
mod pending_diff;
mod serialize;
mod time;
mod undo_operation;

pub use backend::Backend;
pub use change::{Change, UnencodedChange};
pub use error::AutomergeError;
pub use op::Operation;
pub use op_type::OpType;
