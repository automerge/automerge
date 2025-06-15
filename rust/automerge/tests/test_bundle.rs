use automerge::{transaction::Transactable, Automerge, AutomergeError, ReadDoc, ROOT};

#[test]
fn save_and_load_bundle() {
    let mut doc = Automerge::new();
    doc.transact::<_, _, AutomergeError>(|tx| {
        tx.put(ROOT, "foo", "bar")?;
        Ok(())
    })
    .unwrap();

    let mut fork = doc.fork();

    let start = doc.get_heads();

    for i in 0..10 {
        doc.transact::<_, _, AutomergeError>(|tx| {
            tx.put(ROOT, "i", i)?;
            Ok(())
        })
        .unwrap();
    }

    let end = doc.get_heads();

    let bundle = doc.save_bundle(Some(&start), Some(&end));

    fork.load_incremental(&bundle).unwrap();

    assert_eq!(fork.get(ROOT, "foo").unwrap().unwrap().0, "bar".into());
    assert_eq!(fork.get(ROOT, "i").unwrap().unwrap().0, 9.into());
}
