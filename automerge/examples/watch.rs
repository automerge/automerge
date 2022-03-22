use automerge::transaction::Transactable;
use automerge::Automerge;
use automerge::ChangeHash;
use automerge::ObjId;
use automerge::ROOT;

fn main() {
    let mut doc = Automerge::new();
    let heads1 = doc.get_heads();

    // a simple scalar change in the root object
    let mut tx = doc.transaction();
    tx.set(ROOT, "hello", "world").unwrap();
    let heads2 = tx.commit();
    get_changes(&heads1, &doc);

    let mut tx = doc.transaction();
    let map = tx
        .set_object(ROOT, "my new map", automerge::ObjType::Map)
        .unwrap();
    tx.set(&map, "blah", 1).unwrap();
    tx.set(&map, "blah2", 1).unwrap();
    let list = tx
        .set_object(&map, "my list", automerge::ObjType::List)
        .unwrap();
    // tx.insert(&list, 0, "yay").unwrap();
    let m = tx.insert_object(&list, 0, automerge::ObjType::Map).unwrap();
    tx.set(&m, "hi", 2).unwrap();
    tx.insert(&list, 1, "woo").unwrap();
    let m = tx.insert_object(&list, 2, automerge::ObjType::Map).unwrap();
    tx.set(&m, "hi", 2).unwrap();
    let _heads3 = tx.commit();
    get_changes(&[heads2], &doc);

    // now if a peer were to send us a change that added a key in map we wouldn't know the path to
    // the change or we might not have a reference to the map objid.
}

fn get_changes(heads: &[ChangeHash], doc: &Automerge) {
    let changes = doc.get_changes(heads);
    // changes should be in topological order
    for change in changes {
        let change = change.decode();
        for op in change.operations {
            // get the object that it changed
            let obj = doc.import(&op.obj.to_string()).unwrap();
            // get the prop too
            let prop = format!("{:?}", op.key);
            println!("{:?}", op);
            println!(
                "{} {:?} in obj {:?}, object path {:?}",
                if op.insert { "inserted" } else { "changed" },
                prop,
                obj,
                get_path_for_obj(doc, &obj)
            );
        }
    }
}

fn get_path_for_obj(doc: &Automerge, obj: &ObjId) -> String {
    let mut s = String::new();
    let mut obj = obj.clone();
    while let Some((parent, key)) = doc.parent_object(obj) {
        s = format!("{}/{}", key, s);
        obj = parent;
    }
    s
}
