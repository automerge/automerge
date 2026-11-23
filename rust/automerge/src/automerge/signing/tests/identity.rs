//! Runtime identity cannot disagree with its construction-time verifier.

use super::*;
use crate::{AutoCommit, Config, LoadOptions};

#[test]
fn required_handle_borrows_the_same_prepared_identity() {
    let identity = author(&key(7));
    let mut doc = Automerge::from_config(Config::default().signing(identity.clone()).unwrap());
    let before = format!("{doc:?}");
    let first = std::ptr::from_ref(&doc.try_signing().unwrap().state.author);
    for _ in 0..2 {
        let handle = doc.try_signing().unwrap();
        assert_eq!(std::ptr::from_ref(&handle.state.author), first);
        assert_eq!(handle.state.author.author(), &identity);
    }
    assert_eq!(format!("{doc:?}"), before);
}

#[test]
fn rejecting_a_changed_signing_author_keeps_the_autocommit_transaction_open() {
    let a = author(&key(7));
    let b = author(&key(8));
    let mut doc = AutoCommit::from_config(Config::default().signing(a.clone()).unwrap());
    doc.put(ROOT, "pending", 1).unwrap();
    let before = format!("{doc:?}");
    assert!(doc.fork_with_author(Some(b)).is_err());
    assert_eq!(format!("{doc:?}"), before);
    assert_eq!(doc.pending_ops(), 1);
    let mut same = doc.fork_with_author(Some(a)).unwrap();
    assert_eq!(doc.pending_ops(), 0);
    assert_eq!(same.try_signing().unwrap().required_signatures().len(), 1);
}

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
