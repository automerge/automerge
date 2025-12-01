use automerge::transaction::CommitOptions;
use automerge::transaction::Transactable;
use automerge::AutomergeError;
use automerge::ObjType;
use automerge::{Automerge, ReadDoc, ROOT};

// Based on https://automerge.org/docs/reference/documents/lists/
fn main() {
    let mut doc1 = Automerge::new();
    let (cards, card1) = doc1
        .transact_with::<_, _, AutomergeError, _>(
            |_| CommitOptions::default().with_message("Add card".to_owned()),
            |tx| {
                let cards = tx.put_object(ROOT, "cards", ObjType::List).unwrap();
                let card1 = tx.insert_object(&cards, 0, ObjType::Map)?;
                tx.put(&card1, "title", "Rewrite everything in Clojure")?;
                tx.put(&card1, "done", false)?;
                let card2 = tx.insert_object(&cards, 0, ObjType::Map)?;
                tx.put(&card2, "title", "Rewrite everything in Haskell")?;
                tx.put(&card2, "done", false)?;
                Ok((cards, card1))
            },
        )
        .unwrap()
        .result;

    let mut doc2 = Automerge::new();
    doc2.merge(&mut doc1).unwrap();

    let binary = doc1.save();
    let mut doc2 = Automerge::load(&binary).unwrap();

    doc1.transact_with::<_, _, AutomergeError, _>(
        |_| CommitOptions::default().with_message("Mark card as done".to_owned()),
        |tx| {
            tx.put(&card1, "done", true)?;
            Ok(())
        },
    )
    .unwrap();

    doc2.transact_with::<_, _, AutomergeError, _>(
        |_| CommitOptions::default().with_message("Delete card".to_owned()),
        |tx| {
            tx.delete(&cards, 0)?;
            Ok(())
        },
    )
    .unwrap();

    doc1.merge(&mut doc2).unwrap();

    for change in doc1.get_changes(&[]) {
        let length = doc1.length_at(&cards, &[change.hash()]);
        println!("{} {}", change.message().unwrap(), length);
    }
}
