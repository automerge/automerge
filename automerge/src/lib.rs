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
//! println!("{:?}", doc.state().to_json());
//! ```
//!
//! Generate changes like so:
//!
//! ```rust,no_run
//! # use automerge::{Document, Change, ChangeRequest, Path, Value};
//! let mut doc = Document::init();
//! let json_value: serde_json::Value = serde_json::from_str(
//!     r#"
//!     {
//!         "cards_by_id": {},
//!         "size_of_cards": 12.0,
//!         "numRounds": 11.0,
//!         "cards": [1.0, false]
//!     }
//! "#,
//! )
//! .unwrap();
//! doc.create_and_apply_change(
//!     Some("Some change".to_string()),
//!     vec![ChangeRequest::Set {
//!         path: Path::root().key("the-state".to_string()),
//!         value: Value::from_json(&json_value),
//!     }],
//! )
//! .unwrap();
//! let expected: serde_json::Value = serde_json::from_str(
//!     r#"
//!     {
//!         "the-state": {
//!             "cards_by_id": {},
//!             "size_of_cards": 12.0,
//!             "numRounds": 11.0,
//!             "cards": [1.0, false]
//!         }
//!     }
//! "#,
//! )
//! .unwrap();
//! assert_eq!(expected, doc.state().to_json());
//! ```
mod actor_histories;
mod change_context;
mod change_request;
mod concurrent_operations;
mod document;
mod error;
mod object_store;
mod op_set;
mod operation_with_metadata;
mod protocol;
mod value;

pub use change_request::{ChangeRequest, Path, ListIndex};
pub use document::Document;
pub use error::AutomergeError;
pub use protocol::Change;
pub use value::Value;
