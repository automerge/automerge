use automerge::{transaction::Transactable, AutoCommit, Change, ReadDoc, SaveOptions, Value, ROOT};

struct Orphans {
    doc: AutoCommit,
    missing_change: Change,
}

/// Create a document with an orphan change
fn doc_with_orphans() -> Orphans {
    let mut doc1 = AutoCommit::new();
    doc1.put(&ROOT, "key", "value").unwrap();

    // Create two changes remotely
    let mut doc2 = doc1.fork();
    doc2.put(&ROOT, "key", "value2").unwrap();
    let change1 = doc2.get_last_local_change().unwrap().clone();
    doc2.put(&ROOT, "key", "value3").unwrap();
    let change2 = doc2.get_last_local_change().unwrap().clone();

    // Apply the second change, which means it will be orphaned because doc1 doesn't have the first
    // change
    doc1.apply_changes(vec![change2]).unwrap();
    Orphans {
        doc: doc1,
        missing_change: change1,
    }
}

#[test]
fn save_orphaned_changes() {
    let Orphans {
        mut doc,
        missing_change,
    } = doc_with_orphans();

    let saved = doc.save();
    let mut loaded = AutoCommit::load(&saved).unwrap();

    loaded.apply_changes(vec![missing_change]).unwrap();

    // Both changes should now have been applied so the end result should be value3
    assert_eq!(
        loaded.get(&ROOT, "key").unwrap().unwrap().0,
        Value::from("value3")
    );
}

#[test]
fn discard_orphans() {
    let Orphans {
        mut doc,
        missing_change,
    } = doc_with_orphans();

    let saved = doc.save_with_options(SaveOptions {
        retain_orphans: false,
        ..Default::default()
    });
    let mut loaded = AutoCommit::load(&saved).unwrap();

    loaded.apply_changes(vec![missing_change]).unwrap();

    // The depeendent change should never have been included and so we should only see the value
    // from the second change
    assert_eq!(
        loaded.get(&ROOT, "key").unwrap().unwrap().0,
        Value::from("value2")
    );
}

#[test]
fn load_incremental_change_without_deps_throws() {
    let mut doc = AutoCommit::new();
    doc.put(&ROOT, "key", "value").unwrap();
    let _ = doc.save_incremental();

    doc.put(&ROOT, "key", "value2").unwrap();
    let orphan = doc.save_incremental();
    if let Err(e) = AutoCommit::load(&orphan) {
        assert_eq!(e, automerge::AutomergeError::MissingDeps);
    } else {
        panic!("loading an orphan change without a document chunk as first chunk should fail");
    }
}
