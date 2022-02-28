use automerge::transaction::CommitOptions;
use automerge::transaction::Transactable;
use automerge::{Automerge, ROOT};
use automerge::{AutomergeError, Value};

// Based on https://automerge.github.io/docs/quickstart
fn main() {
    let mut doc1 = Automerge::new();
    let (cards, card1) = doc1
        .transact_with::<_, _, AutomergeError, _>(
            |tx| {
                let cards = tx.set(&ROOT, "cards", Value::list()).unwrap().unwrap();
                let card1 = tx.insert(&cards, 0, Value::map())?.unwrap();
                tx.set(&card1, "title", "Rewrite everything in Clojure")?;
                tx.set(&card1, "done", false)?;
                let card2 = tx.insert(&cards, 0, Value::map())?.unwrap();
                tx.set(&card2, "title", "Rewrite everything in Haskell")?;
                tx.set(&card2, "done", false)?;
                Ok((cards, card1))
            },
            || CommitOptions::default().with_message("Add card".to_owned()),
        )
        .unwrap()
        .into_result();

    let mut doc2 = Automerge::new();
    doc2.merge(&mut doc1).unwrap();

    let binary = doc1.save().unwrap();
    let mut doc2 = Automerge::load(&binary).unwrap();

    doc1.transact_with::<_, _, AutomergeError, _>(
        |tx| {
            tx.set(&card1, "done", true)?;
            Ok(())
        },
        || CommitOptions::default().with_message("Mark card as done".to_owned()),
    )
    .unwrap();

    doc2.transact_with::<_, _, AutomergeError, _>(
        |tx| {
            tx.del(&cards, 0)?;
            Ok(())
        },
        || CommitOptions::default().with_message("Delete card".to_owned()),
    )
    .unwrap();

    doc1.merge(&mut doc2).unwrap();

    for change in doc1.get_changes(&[]) {
        let length = doc1.length_at(&cards, &[change.hash]);
        println!("{} {}", change.message().unwrap(), length);
    }
}
