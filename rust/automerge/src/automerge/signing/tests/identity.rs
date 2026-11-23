//! Runtime identity cannot disagree with its construction-time verifier.

use super::*;
use crate::{AutoCommit, Config, LoadOptions};

/// Taking a signing handle borrows the identity prepared at construction and
/// leaves the document untouched.
#[test]
fn required_handle_borrows_the_same_prepared_identity() {
    let identity = author(&key(7));
    let mut doc = Automerge::from_config(Config::default().signing(identity.clone()).unwrap());
    // The `Debug` string is the "document unchanged" oracle; the raw-pointer
    // equality below only pins that the handle borrows (does not re-prepare)
    // the stored author.
    let before = format!("{doc:?}");
    let first = std::ptr::from_ref(&doc.try_signing().unwrap().state.author);
    for _ in 0..2 {
        let handle = doc.try_signing().unwrap();
        assert_eq!(std::ptr::from_ref(&handle.state.author), first);
        assert_eq!(handle.state.author.author(), &identity);
    }
    assert_eq!(format!("{doc:?}"), before);
}

/// Loading unsigned history with a configured author attributes the migration
/// change to that author and keeps it for later local commits.
#[test]
fn unsigned_load_migration_installs_the_configured_author_before_local_history() {
    let mut old = AutoCommit::new();
    old.put(ROOT, "string", "legacy").unwrap();
    let bytes = old.save();
    let a = Author::from(vec![1, 2, 3]);
    let mut loaded = AutoCommit::load_with_options(
        &bytes,
        LoadOptions::new()
            .author(a.clone())
            .migrate_strings(crate::StringMigration::ConvertToText),
    )
    .unwrap();
    assert_eq!(
        loaded.get_last_local_change().unwrap().author(),
        Some(a.clone())
    );
    loaded.put(ROOT, "next", 1).unwrap();
    loaded.commit();
    assert_eq!(loaded.get_author(), Some(&a));
}
