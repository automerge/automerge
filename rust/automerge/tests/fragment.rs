use automerge::{transaction::Transactable, Automerge, AutomergeError, ObjType, ReadDoc, ROOT};

mod fragment {
    use std::hash::{DefaultHasher, Hasher};

    use automerge::Automerge;
    use testresult::TestResult;

    fn fixture(name: &str) -> Vec<u8> {
        std::fs::read("./tests/fixtures/".to_owned() + name).unwrap()
    }

    #[test]
    fn everything_is_a_boundary() -> TestResult {
        let doc = Automerge::load(&fixture("diamond_history.automerge"))?;
        let head = doc.get_heads()[0];
        let frag = doc.fragment(head, |_change| true)?;

        // dbg!(frag.head_hash());
        // dbg!(frag.boundary().keys().collect::<Vec<_>>());

        assert!(frag.members().contains(&head));
        assert!(!frag.boundary().contains_key(&head));

        assert_eq!(frag.head_hash(), head);
        assert_eq!(frag.members().len(), 1);
        assert_eq!(frag.boundary().len(), 2);

        Ok(())
    }

    // #[test]
    // fn nothing_is_a_boundary() -> TestResult {
    //     let doc = Automerge::load(&fixture("diamond_history.automerge"))?;
    //     let head = doc.get_heads()[0];
    //     let frag = doc.fragment(head, |_change| false)?;

    //     assert!(frag.members().contains(&head));
    //     assert!(!frag.boundary().contains_key(&head));

    //     assert_eq!(frag.head_hash(), head);
    //     assert_eq!(frag.members().len(), 104);
    //     assert_eq!(frag.boundary().len(), 0);

    //     Ok(())
    // }

    // #[test]
    // fn first_hash_byte_is_zero() -> TestResult {
    //     let doc = Automerge::load(&fixture("diamond_history.automerge"))?;
    //     let head = doc.get_heads()[0];
    //     let frag = doc.fragment(head, |change| {
    //         let hash_bytes: [u8; 32] = change.hash().0;
    //         hash_bytes[0] == 0
    //     })?;

    //     assert!(frag.members().contains(&head));
    //     assert!(!frag.boundary().contains_key(&head));

    //     assert_eq!(frag.head_hash(), head);
    //     assert_eq!(frag.members().len(), 104);
    //     assert_eq!(frag.boundary().len(), 0);

    //     Ok(())
    // }

    // #[test]
    // fn last_hash_byte_is_zero() -> TestResult {
    //     let doc = Automerge::load(&fixture("diamond_history.automerge"))?;
    //     let head = doc.get_heads()[0];
    //     let frag = doc.fragment(head, |change| {
    //         let hash_bytes: [u8; 32] = change.hash().0;
    //         hash_bytes[31] == 0
    //     })?;

    //     assert!(frag.members().contains(&head));
    //     assert!(!frag.boundary().contains_key(&head));

    //     assert_eq!(frag.head_hash(), head);
    //     assert_eq!(frag.members().len(), 23);
    //     assert_eq!(frag.boundary().len(), 1);

    //     Ok(())
    // }

    // #[test]
    // fn hash_checksum_greater_than_200() -> TestResult {
    //     super::gen_doc()?;
    //     let doc = Automerge::load(&fixture("diamond_history.automerge"))?;
    //     let head = doc.get_heads()[0];
    //     let frag = doc.fragment(head, |change| {
    //         dbg!(change.deps().len());
    //         if change.deps().len() > 1 {
    //             dbg!(change.hash());
    //         }
    //         false
    //         // let hash_bytes: [u8; 32] = change.hash().0;
    //         // let mut hasher = DefaultHasher::new();
    //         // hasher.write(&hash_bytes);
    //         // let silly_checksum = hasher.finish() as u8;
    //         // silly_checksum == 219
    //     })?;

    //     let serialized = serde_json::to_string(&automerge::AutoSerde::from(&doc)).unwrap();
    //     dbg!(serialized);

    //     assert!(frag.members().contains(&head));
    //     assert!(!frag.boundary().contains_key(&head));

    //     assert_eq!(frag.head_hash(), head);
    //     // assert_eq!(frag.members().len(), 70);
    //     assert_eq!(frag.boundary().len(), 2);

    //     Ok(())
    // }
}

pub fn gen_doc() -> Result<Automerge, AutomergeError> {
    let mut doc = Automerge::new();

    doc.transact(|tx| {
        tx.put(ROOT, "a", 1)?;

        let nested = tx
            .put_object(ROOT, "nested", ObjType::List)
            .expect("put failed");
        tx.insert(&nested, 0, "hello")?;
        tx.insert(&nested, 1, "world")?;

        Ok::<_, AutomergeError>(())
    })
    .map_err(|te| te.error)?;

    let mut left = doc.fork();
    let mut right = doc.fork();

    doc.transact(|tx| {
        tx.put(ROOT, "z", "left and right don't have this!")?;
        Ok::<_, AutomergeError>(())
    })
    .map_err(|te| te.error)?;

    left.transact(|tx| {
        tx.put(ROOT, "b", 0)?;
        tx.put(ROOT, "c", 0)?;
        Ok::<_, AutomergeError>(())
    })
    .map_err(|te| te.error)?;

    // Right
    right
        .transact(|tx| {
            tx.put(ROOT, "c", 3)?;
            tx.put(ROOT, "d", 4)?;
            let (_nested_read, nested_write) = tx
                .get(ROOT, "nested")?
                .expect("nested value missing at expected path");
            tx.insert(&nested_write, 2, "everyone")?;
            Ok::<_, AutomergeError>(())
        })
        .map_err(|te| te.error)?;

    for i in 0..100 {
        right
            .transact(|tx| {
                tx.put(ROOT, format!("iter-{i}"), i).expect("put failed");
                Ok::<_, AutomergeError>(())
            })
            .expect("transaction failed");
    }

    doc.merge(&mut left)?;
    doc.merge(&mut right)?;

    doc.transact(|tx| {
        tx.put(ROOT, "e", 5)?;
        tx.put(ROOT, "f", 6)?;
        Ok::<_, AutomergeError>(())
    })
    .map_err(|te| te.error)?;

    for i in 0..20 {
        doc.transact(|tx| {
            tx.put(ROOT, format!("iter-{i}"), i).expect("put failed");
            Ok::<_, AutomergeError>(())
        })
        .map_err(|te| te.error)?;
    }

    std::fs::write("./tests/fixtures/diamond_history.automerge", doc.save());

    Ok(doc)
}
