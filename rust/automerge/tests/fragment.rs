mod fragment {
    use std::sync::OnceLock;

    use automerge::{transaction::Transactable, Automerge, AutomergeError, ObjType, ReadDoc, ROOT};

    static DOC_FIXTURE: OnceLock<Automerge> = OnceLock::new();
    fn doc_fixture() -> &'static Automerge {
        DOC_FIXTURE.get_or_init(|| {
            let mut doc = Automerge::new();

            doc.transact(|tx| {
                tx.put(ROOT, "a", 1).expect("put failed");

                let nested = tx
                    .put_object(ROOT, "nested", ObjType::List)
                    .expect("put failed");
                tx.insert(&nested, 0, "hello").expect("put failed");
                tx.insert(&nested, 1, "world").expect("put failed");

                Ok::<_, AutomergeError>(())
            })
            .expect("transaction failed");

            let mut left = doc.fork();
            let mut right = doc.fork();

            left.transact(|tx| {
                tx.put(ROOT, "b", 0).expect("put failed");
                tx.put(ROOT, "c", 0).expect("put failed");
                Ok::<_, AutomergeError>(())
            })
            .expect("transaction failed");

            // Right
            right
                .transact(|tx| {
                    tx.put(ROOT, "c", 3).expect("put failed");
                    tx.put(ROOT, "d", 4).expect("put failed");
                    let (_nested_read, nested_write) = tx
                        .get(ROOT, "nested")
                        .expect("get failed")
                        .expect("nested value missing at expected path");
                    tx.insert(&nested_write, 2, "everyone").expect("put failed");
                    Ok::<_, AutomergeError>(())
                })
                .expect("transaction failed");

            for i in 0..100 {
                right
                    .transact(|tx| {
                        tx.put(ROOT, format!("iter-{i}"), i).expect("put failed");
                        Ok::<_, AutomergeError>(())
                    })
                    .expect("transaction failed");
            }

            doc.merge(&mut left).expect("merge failed");
            doc.merge(&mut right).expect("merge failed");

            doc.transact(|tx| {
                tx.put(ROOT, "e", 5).expect("put failed");
                tx.put(ROOT, "f", 6).expect("put failed");
                Ok::<_, AutomergeError>(())
            })
            .expect("transaction failed");

            for i in 0..20 {
                doc.transact(|tx| {
                    tx.put(ROOT, format!("iter-{i}"), i).expect("put failed");
                    Ok::<_, AutomergeError>(())
                })
                .expect("transaction failed");
            }

            doc
        })
    }

    #[test]
    fn everything_is_a_boundary() {
        let doc = doc_fixture();
        let head = doc.get_heads()[0];
        let frag = doc.fragment(head, |_change| true).expect("FIXME");

        assert_eq!(frag.head_hash(), head);
        assert!(frag.members().contains(&head));
        assert_eq!(frag.members().len(), 1);
        assert_eq!(frag.boundary().len(), 2);
    }

    #[test]
    fn nothing_is_a_boundary() {
        let doc = doc_fixture();
        let heads = doc.get_heads();
        let frag = doc.fragment(heads[0], |_change| false).expect("FIXME");

        assert_eq!(frag.head_hash(), heads[0]);
        assert!(frag.members().contains(&heads[0]));
        assert_eq!(frag.members().len(), 104);
        assert_eq!(frag.boundary().len(), 0);
    }
}
