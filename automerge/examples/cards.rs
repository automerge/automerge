use anyhow::Result;
use automerge::{
    Backend, Change, Frontend, InvalidChangeRequest, LocalChange, MutableDocument, Path, Primitive,
    Value,
};
use serde_json::json;

#[derive(Default)]
pub struct Automerge {
    backend: Backend,
    frontend: Frontend<fn() -> Option<i64>>,
    stream: Vec<Change>,
}

impl Automerge {
    pub fn apply_changes(&mut self, changes: &[Change]) -> Result<()> {
        for change in changes {
            let patch = self.backend.apply_changes(vec![change.clone()])?;
            self.frontend.apply_patch(patch)?;
        }
        Ok(())
    }

    pub fn change<F, O>(&mut self, msg: &'static str, cb: F) -> Result<O>
    where
        F: FnOnce(&mut dyn MutableDocument) -> Result<O, InvalidChangeRequest>,
    {
        let (output, change) = self.frontend.change(Some(msg.to_string()), cb)?;
        if let Some(change) = change {
            let (patch, change) = self.backend.apply_local_change(change)?;
            self.frontend.apply_patch(patch)?;
            self.stream.push(change.clone());
        }
        Ok(output)
    }

    pub fn state(&mut self) -> &Value {
        self.frontend.state()
    }

    pub fn changes(&self) -> &[Change] {
        &self.stream
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Let's say doc1 is the application state on device 1.
    // Further down we'll simulate a second device.
    // We initialize the document to initially contain an empty list of cards.
    let mut doc1 = Automerge::default();
    doc1.change("Initial state", |doc| {
        doc.add_change(LocalChange::set(
            Path::root(),
            Value::from_json(&json!({ "cards": [] })),
        ))
    })?;

    // To change it, you need to call `change` with a callback in which you can
    // mutate the state. You can also include a human-readable description of the
    // change, like a commit message, which is stored in the change history (see below).

    doc1.change("Add card", |doc| {
        doc.add_change(LocalChange::insert(
            Path::root().key("cards").index(0),
            Value::from_json(&json!({ "title": "Rewrite everything in Clojure", "done": false })),
        ))
    })?;

    // Now the state of doc1 is:
    // { cards: [ { title: 'Rewrite everything in Clojure', done: false } ] }

    doc1.change("Add another card", |doc| {
        doc.add_change(LocalChange::insert(
            Path::root().key("cards").index(0),
            Value::from_json(&json!({ "title": "Rewrite everything in Haskell", "done": false })),
        ))
    })?;

    // { cards:
    //    [ { title: 'Rewrite everything in Haskell', done: false },
    //      { title: 'Rewrite everything in Clojure', done: false } ] }

    // Now let's simulate another device, whose application state is doc2. We
    // initialise it separately, and merge doc1 into it. After merging, doc2 has
    // a copy of all the cards in doc1.

    let mut doc2 = Automerge::default();

    doc2.apply_changes(doc1.changes())?;

    // Now make a change on device 1:
    doc1.change("Mark card as done", |doc| {
        doc.add_change(LocalChange::set(
            Path::root().key("cards").index(0).key("done"),
            Value::Primitive(Primitive::Boolean(true)),
        ))
    })?;

    // { cards:
    //    [ { title: 'Rewrite everything in Haskell', done: true },
    //      { title: 'Rewrite everything in Clojure', done: false } ] }

    // And, unbeknownst to device 1, also make a change on device 2:
    doc2.change("Delete card", |doc| {
        doc.add_change(LocalChange::delete(Path::root().key("cards").index(1)))
    })?;

    // { cards: [ { title: 'Rewrite everything in Haskell', done: false } ] }

    // Now comes the moment of truth. Let's merge the changes from device 2 back
    // into device 1. You can also do the merge the other way round, and you'll get
    // the same result. The merged result remembers that 'Rewrite everything in
    // Haskell' was set to true, and that 'Rewrite everything in Clojure' was
    // deleted:

    doc1.apply_changes(doc2.changes())?;

    // { cards: [ { title: 'Rewrite everything in Haskell', done: true } ] }
    println!("{}", doc1.state().to_json());

    Ok(())
}
