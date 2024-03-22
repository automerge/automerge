use automerge::patches::TextRepresentation;
use automerge::transaction::CommitOptions;
use automerge::transaction::Transactable;
use automerge::Automerge;
use automerge::AutomergeError;
use automerge::ROOT;
use automerge::{Patch, PatchAction, PatchLog};

fn main() {
    let mut doc = Automerge::new();

    // a simple scalar change in the root object
    let mut result = doc
        .transact_and_log_patches_with::<_, _, AutomergeError, _>(
            TextRepresentation::String,
            |_result| CommitOptions::default(),
            |tx| {
                tx.put(ROOT, "hello", "world").unwrap();
                Ok(())
            },
        )
        .unwrap();
    get_changes(&doc, doc.make_patches(&mut result.patch_log));

    let mut tx = doc.transaction_log_patches(PatchLog::active(TextRepresentation::String));
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
    let (_heads3, mut patch_log) = tx.commit_with(CommitOptions::default());
    let patches = doc.make_patches(&mut patch_log);
    get_changes(&doc, patches);
}

fn get_changes(_doc: &Automerge, patches: Vec<Patch>) {
    for Patch { obj, path, action } in patches {
        match action {
            PatchAction::PutMap { key, value, .. } => {
                println!(
                    "put {:?} at {:?} in obj {:?}, object path {:?}",
                    value, key, obj, path,
                )
            }
            PatchAction::PutSeq { index, value, .. } => {
                println!(
                    "put {:?} at {:?} in obj {:?}, object path {:?}",
                    value, index, obj, path,
                )
            }
            PatchAction::Insert { index, values, .. } => {
                println!(
                    "insert {:?} at {:?} in obj {:?}, object path {:?}",
                    values, index, obj, path,
                )
            }
            PatchAction::SpliceText { index, value, .. } => {
                println!(
                    "splice '{:?}' at {:?} in obj {:?}, object path {:?}",
                    value, index, obj, path,
                )
            }
            PatchAction::Increment { prop, value, .. } => {
                println!(
                    "increment {:?} in obj {:?} by {:?}, object path {:?}",
                    prop, obj, value, path,
                )
            }
            PatchAction::DeleteMap { key, .. } => {
                println!("delete {:?} in obj {:?}, object path {:?}", key, obj, path,)
            }
            PatchAction::DeleteSeq { index, .. } => println!(
                "delete {:?} in obj {:?}, object path {:?}",
                index, obj, path,
            ),
            PatchAction::Mark { marks } => {
                println!("mark {:?} in obj {:?}, object path {:?}", marks, obj, path,)
            }
            PatchAction::Conflict { prop } => {
                println!(
                    "conflict on {:?} in obj {:?}, object path {:?}",
                    prop, obj, path,
                )
            }
        }
    }
}
