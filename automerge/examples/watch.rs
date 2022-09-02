use automerge::transaction::CommitOptions;
use automerge::transaction::Transactable;
use automerge::Automerge;
use automerge::AutomergeError;
use automerge::Patch;
use automerge::VecOpObserver;
use automerge::ROOT;

fn main() {
    let mut doc = Automerge::new();

    // a simple scalar change in the root object
    let mut result = doc
        .transact_with::<_, _, AutomergeError, _, VecOpObserver>(
            |_result| CommitOptions::default(),
            |tx| {
                tx.put(ROOT, "hello", "world").unwrap();
                Ok(())
            },
        )
        .unwrap();
    get_changes(&doc, result.op_observer.take_patches());

    let mut tx = doc.transaction_with_observer(VecOpObserver::default());
    let map = tx
        .put_object(ROOT, "my new map", automerge::ObjType::Map)
        .unwrap();
    tx.put(&map, "blah", 1).unwrap();
    tx.put(&map, "blah2", 1).unwrap();
    let list = tx
        .put_object(&map, "my list", automerge::ObjType::List)
        .unwrap();
    tx.insert(&list, 0, "yay").unwrap();
    let m = tx.insert_object(&list, 0, automerge::ObjType::Map).unwrap();
    tx.put(&m, "hi", 2).unwrap();
    tx.insert(&list, 1, "woo").unwrap();
    let m = tx.insert_object(&list, 2, automerge::ObjType::Map).unwrap();
    tx.put(&m, "hi", 2).unwrap();
    let patches = tx.op_observer.take_patches();
    let _heads3 = tx.commit_with(CommitOptions::default());
    get_changes(&doc, patches);
}

fn get_changes(doc: &Automerge, patches: Vec<Patch>) {
    for patch in patches {
        match patch {
            Patch::Put {
                obj, prop, value, ..
            } => {
                println!(
                    "put {:?} at {:?} in obj {:?}, object path {:?}",
                    value,
                    prop,
                    obj,
                    doc.path_to_object(&obj)
                )
            }
            Patch::Insert {
                obj, index, value, ..
            } => {
                println!(
                    "insert {:?} at {:?} in obj {:?}, object path {:?}",
                    value,
                    index,
                    obj,
                    doc.path_to_object(&obj)
                )
            }
            Patch::Increment {
                obj, prop, value, ..
            } => {
                println!(
                    "increment {:?} in obj {:?} by {:?}, object path {:?}",
                    prop,
                    obj,
                    value,
                    doc.path_to_object(&obj)
                )
            }
            Patch::Delete { obj, prop, .. } => println!(
                "delete {:?} in obj {:?}, object path {:?}",
                prop,
                obj,
                doc.path_to_object(&obj)
            ),
        }
    }
}
