use automerge::transaction::Transactable;
use automerge::{Automerge, ObjType, ReadDoc, ROOT};

fn main() {
    let mut doc = Automerge::new();
    let before = doc.get_heads();
    let mut tx = doc.transaction();
    let map = tx.put_object(ROOT, "my new map", ObjType::Map).unwrap();
    tx.put(&map, "hello", "world").unwrap();
    tx.commit();
    for patch in doc.diff(&before, &doc.get_heads()) {
        println!("{patch:?}");
    }
}
