#![no_main]
use arbitrary::Arbitrary;
use automerge::transaction::Transactable;
use automerge::Automerge;
use automerge::ObjType;
use automerge::ScalarValue;
use automerge::ROOT;
use libfuzzer_sys::fuzz_target;

#[derive(Debug, Arbitrary)]
enum Action {
    SetMap(String, ScalarValue),
    DelMap(String),
    InsertText(usize, char),
    DelText(usize),
}

// Fuzz the load operation on an Automerge document.
fuzz_target!(|actions: Vec<Action>| {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let list = tx.put_object(ROOT, "list", ObjType::List).unwrap();
    let map = tx.put_object(ROOT, "map", ObjType::Map).unwrap();
    let mut list_len = 0;
    for action in actions {
        match action {
            Action::SetMap(key, value) => {
                if !key.is_empty() {
                    tx.put(&map, key, value.clone()).unwrap();
                }
            }
            Action::DelMap(key) => {
                if !key.is_empty() {
                    tx.delete(&map, key).unwrap();
                }
            }
            Action::InsertText(index, c) => {
                let index = index % (list_len + 1);
                list_len += 1;
                tx.insert(&list, index, c).unwrap();
            }
            Action::DelText(index) => {
                if list_len > 0 {
                    let index = index % list_len;
                    list_len -= 1;
                    tx.delete(&list, index).unwrap();
                }
            }
        }
    }
    tx.commit();
    let bytes = doc.save();
    let mut doc2 = Automerge::load(&bytes).unwrap();
    let bytes2 = doc2.save();
    assert_eq!(bytes, bytes2);
});
