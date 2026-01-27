//! # Automerge
//!
//! Automerge is a library of data structures for building collaborative,
//! [local-first](https://www.inkandswitch.com/local-first/) applications. The
//! idea of automerge is to provide a data structure which is quite general
//! \- consisting of nested key/value maps and/or lists - which can be modified
//! entirely locally but which can at any time be merged with other instances of
//! the same data structure.
//!
//! In addition to the core data structure (which we generally refer to as a
//! "document"), we also provide an implementation of a sync protocol (in
//! [`crate::sync`]) which can be used over any reliable in-order transport; and
//! an efficient binary storage format.
//!
//! This crate is organised around two representations of a document -
//! [`Automerge`] and [`AutoCommit`]. The difference between the two is that
//! [`AutoCommit`] manages transactions for you. Both of these representations
//! implement [`ReadDoc`] for reading values from a document and provide access
//! to a [`sync::SyncDoc`] implementation (`Automerge` implements it directly
//! whilst [`AutoCommit`] provides [`AutoCommit::sync`]) for taking part in the
//! sync protocol. [`AutoCommit`] directly implements
//! [`transaction::Transactable`] for making changes to a document, whilst
//! [`Automerge`] requires you to explicitly create a
//! [`transaction::Transaction`].
//!
//! NOTE: The API this library provides for modifying data is quite low level
//! (somewhat analogous to directly creating JSON values rather than using
//! [`serde`] derive macros or equivalent). If you're writing a Rust application which uses automerge
//! you may want to look at [autosurgeon](https://github.com/automerge/autosurgeon).
//!
//! ## Data Model
//!
//! An automerge document is a map from strings to values
//! ([`Value`]) where values can be either
//!
//! * A nested composite value which is either
//!   * A map from strings to values ([`ObjType::Map`])
//!   * A list of values ([`ObjType::List`])
//!   * A text object (a sequence of unicode characters) ([`ObjType::Text`])
//! * A primitive value ([`ScalarValue`]) which is one of
//!   * A string
//!   * A 64 bit floating point number
//!   * A signed 64 bit integer
//!   * An unsigned 64 bit integer
//!   * A boolean
//!   * A counter object (a 64 bit integer which merges by addition)
//!     ([`ScalarValue::Counter`])
//!   * A timestamp (a 64 bit integer which is milliseconds since the unix epoch)
//!
//! All composite values have an ID ([`ObjId`]) which is created when the value
//! is inserted into the document or is the root object ID [`ROOT`]. Values in
//! the document are then referred to by the pair (`object ID`, `key`). The
//! `key` is represented by the [`Prop`] type and is either a string for a maps,
//! or an index for sequences.
//!
//! ### Conflicts
//!
//! There are some things automerge cannot merge sensibly. For example, two
//! actors concurrently setting the key "name" to different values. In this case
//! automerge will pick a winning value in a random but deterministic way, but
//! the conflicting value is still available via the [`ReadDoc::get_all()`] method.
//!
//! ### Change hashes and historical values
//!
//! Like git, points in the history of a document are identified by hash. Unlike
//! git there can be multiple hashes representing a particular point (because
//! automerge supports concurrent changes). These hashes can be obtained using
//! either [`Automerge::get_heads()`] or [`AutoCommit::get_heads()`] (note these
//! methods are not part of [`ReadDoc`] because in the case of [`AutoCommit`] it
//! requires a mutable reference to the document).
//!
//! These hashes can be used to read values from the document at a particular
//! point in history using the various `*_at()` methods on [`ReadDoc`] which take a
//! slice of [`ChangeHash`] as an argument.
//!
//! ### Actor IDs
//!
//! Any change to an automerge document is made by an actor, represented by an
//! [`ActorId`]. An actor ID is any random sequence of bytes but each change by
//! the same actor ID must be sequential. This often means you will want to
//! maintain at least one actor ID per device. It is fine to generate a new
//! actor ID for each change, but be aware that each actor ID takes up space in
//! a document so if you expect a document to be long lived and/or to have many
//! changes then you should try to reuse actor IDs where possible.
//!
//! ### Text Encoding
//!
//! Text is encoded in UTF-8 by default but uses UTF-16 when using the wasm target,
//! you can configure it with the feature `utf16-indexing`.
//!
//! ## Sync Protocol
//!
//! See the [`sync`] module.
//!
//! ## Patches, maintaining materialized state
//!
//! Often you will have some state which represents the "current" state of the document. E.g. some
//! text in a UI which is a view of a text object in the document. Rather than re-rendering this
//! text every single time a change comes in you can use a [`PatchLog`] to capture incremental
//! changes made to the document and then use [`Automerge::make_patches()`] to get a set of patches
//! to apply to the materialized state.
//!
//! Many of the methods on [`Automerge`], [`crate::sync::SyncDoc`] and
//! [`crate::transaction::Transactable`] have a `*_log_patches()` variant which allow you to pass in
//! a [`PatchLog`] to collect these incremental changes.
//!
//! ## Serde serialization
//!
//! Sometimes you just want to get the JSON value of an automerge document. For
//! this you can use [`AutoSerde`], which implements [`serde::Serialize`] for an
//! automerge document.
//!
//! ## Example
//!
//! Let's create a document representing an address book.
//!
//! ```
//! use automerge::{ObjType, AutoCommit, transaction::Transactable, ReadDoc};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut doc = AutoCommit::new();
//!
//! // `put_object` creates a nested object in the root key/value map and
//! // returns the ID of the new object, in this case a list.
//! let contacts = doc.put_object(automerge::ROOT, "contacts", ObjType::List)?;
//!
//! // Now we can insert objects into the list
//! let alice = doc.insert_object(&contacts, 0, ObjType::Map)?;
//!
//! // Finally we can set keys in the "alice" map
//! doc.put(&alice, "name", "Alice")?;
//! doc.put(&alice, "email", "alice@example.com")?;
//!
//! // Create another contact
//! let bob = doc.insert_object(&contacts, 1, ObjType::Map)?;
//! doc.put(&bob, "name", "Bob")?;
//! doc.put(&bob, "email", "bob@example.com")?;
//!
//! // Now we save the address book, we can put this in a file
//! let data: Vec<u8> = doc.save();
//! # Ok(())
//! # }
//! ```
//!
//! Now modify this document on two separate devices and merge the modifications.
//!
//! ```
//! use std::borrow::Cow;
//! use automerge::{ObjType, AutoCommit, transaction::Transactable, ReadDoc};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let mut doc = AutoCommit::new();
//! # let contacts = doc.put_object(automerge::ROOT, "contacts", ObjType::List)?;
//! # let alice = doc.insert_object(&contacts, 0, ObjType::Map)?;
//! # doc.put(&alice, "name", "Alice")?;
//! # doc.put(&alice, "email", "alice@example.com")?;
//! # let bob = doc.insert_object(&contacts, 1, ObjType::Map)?;
//! # doc.put(&bob, "name", "Bob")?;
//! # doc.put(&bob, "email", "bob@example.com")?;
//! # let saved: Vec<u8> = doc.save();
//!
//! // Load the document on the first device and change alices email
//! let mut doc1 = AutoCommit::load(&saved)?;
//! let contacts = match doc1.get(automerge::ROOT, "contacts")? {
//!     Some((automerge::Value::Object(ObjType::List), contacts)) => contacts,
//!     _ => panic!("contacts should be a list"),
//! };
//! let alice = match doc1.get(&contacts, 0)? {
//!    Some((automerge::Value::Object(ObjType::Map), alice)) => alice,
//!    _ => panic!("alice should be a map"),
//! };
//! doc1.put(&alice, "email", "alicesnewemail@example.com")?;
//!
//!
//! // Load the document on the second device and change bobs name
//! let mut doc2 = AutoCommit::load(&saved)?;
//! let contacts = match doc2.get(automerge::ROOT, "contacts")? {
//!    Some((automerge::Value::Object(ObjType::List), contacts)) => contacts,
//!    _ => panic!("contacts should be a list"),
//! };
//! let bob = match doc2.get(&contacts, 1)? {
//!   Some((automerge::Value::Object(ObjType::Map), bob)) => bob,
//!   _ => panic!("bob should be a map"),
//! };
//! doc2.put(&bob, "name", "Robert")?;
//!
//! // Finally, we can merge the changes from the two devices
//! doc1.merge(&mut doc2)?;
//! let bobsname: Option<automerge::Value> = doc1.get(&bob, "name")?.map(|(v, _)| v);
//! assert_eq!(bobsname, Some(automerge::Value::Scalar(Cow::Owned("Robert".into()))));
//!
//! let alices_email: Option<automerge::Value> = doc1.get(&alice, "email")?.map(|(v, _)| v);
//! assert_eq!(alices_email, Some(automerge::Value::Scalar(Cow::Owned("alicesnewemail@example.com".into()))));
//! # Ok(())
//! # }
//! ```
//!
//! ## Cursors, referring to positions in sequences
//!
//! When working with text or other sequences it is often useful to be able to
//! refer to a specific position within the sequence whilst merging remote
//! changes. You can manually do this by maintaining your own offsets and
//! observing patches, but this is error prone. The [`Cursor`] type provides
//! an API for allowing automerge to do the index translations for you. Cursors
//! are created with [`ReadDoc::get_cursor()`] and dereferenced with
//! [`ReadDoc::get_cursor_position()`].

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/automerge/automerge/main/img/brandmark.svg",
    html_favicon_url = "https:///raw.githubusercontent.com/automerge/automerge/main/img/favicon.ico"
)]
#![warn(
    missing_debug_implementations,
    // missing_docs, // TODO: add documentation!
    rust_2018_idioms,
    unreachable_pub,
    bad_style,
    dead_code,
    improper_ctypes,
    non_shorthand_field_patterns,
    no_mangle_generic_items,
    overflowing_literals,
    path_statements,
    patterns_in_fns_without_body,
    unconditional_recursion,
    unused,
    unused_allocation,
    unused_comparisons,
    unused_parens,
    while_true
)]

#[doc(hidden)]
#[macro_export]
macro_rules! log {
     ( $( $t:tt )* ) => {
          {
            use $crate::__log;
            __log!( $( $t )* );
          }
     }
 }

#[cfg(all(feature = "wasm", target_family = "wasm"))]
#[doc(hidden)]
#[macro_export]
macro_rules! __log {
     ( $( $t:tt )* ) => {
         web_sys::console::log_1(&format!( $( $t )* ).into());
     }
 }

#[cfg(not(all(feature = "wasm", target_family = "wasm")))]
#[doc(hidden)]
#[macro_export]
macro_rules! __log {
     ( $( $t:tt )* ) => {
         println!( $( $t )* );
     }
 }

mod autocommit;
mod automerge;
mod autoserde;
mod change;
mod change_graph;
mod clock;
mod columnar;
mod convert;
mod cursor;
pub mod error;
mod exid;
pub mod hydrate;
mod indexed_cache;
pub mod iter;
pub use iter::Span;
#[doc(hidden)]
pub mod legacy;
pub mod marks;
pub mod op_set2;
pub mod patches;
mod read;
mod sequence_tree;
mod storage;
pub mod sync;
mod text_diff;
mod text_value;
pub mod transaction;
mod types;
mod validation;
mod value;
mod view_at;

pub use crate::automerge::{Automerge, LoadOptions, OnPartialLoad, SaveOptions, StringMigration};
pub use autocommit::AutoCommit;
pub use autoserde::AutoSerde;
pub use change::{Change, LoadError as LoadChangeError};
pub use cursor::{Cursor, CursorPosition, MoveCursor, OpCursor};
pub use error::AutomergeError;
pub use error::InvalidActorId;
pub use error::InvalidChangeHashSlice;
pub use error::ViewAtError;
pub use exid::{ExId as ObjId, ObjIdFromBytesError};
pub use legacy::Change as ExpandedChange;
pub use op_set2::{ChangeMetadata, Parent, Parents, ScalarValue as ScalarValueRef, ValueRef};
pub use patches::{Patch, PatchAction, PatchLog};
pub use read::{ReadDoc, Stats};
pub use sequence_tree::SequenceTree;
pub use storage::{Bundle, BundleChange, BundleChangeIter, VerificationMode};
pub use text_value::ConcreteTextValue;
pub use transaction::BlockOrText;
pub use types::{ActorId, ChangeHash, ObjType, OpType, ParseChangeHashError, Prop, TextEncoding};
pub use value::{ScalarValue, Value};
pub use view_at::AutomergeAt;

/// The object ID for the root map of a document
pub const ROOT: ObjId = ObjId::Root;
