//! Get your changes from the javascript library like so:
//!
//! ```javascript
//! doc = ... // create and edit an automerge document
//! let changes = Automerge.getHistory(doc).map(h => h.change)
//! console.log(JSON.stringify(changes, null, 4))
//! ```
//!
//! Then load the changes in rust:
//!
//! ```rust,no_run
//! # use automerge::Change;
//! let changes_str = "<paste the contents of the output here>";
//! let changes: Vec<Change> = serde_json::from_str(changes_str).unwrap();
//! let doc = automerge::Document::load(changes).unwrap();
//! println!("{:?}", doc.state().unwrap());
//! ```
mod document;
mod error;
mod op_set;
mod protocol;

pub use document::Document;
pub use error::AutomergeError;
pub use protocol::Change;
