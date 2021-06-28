// This is how you load Automerge in Node. In a browser, simply including the
// script tag will set up the Automerge object.
use anyhow::Result;
use automerge::{
    Backend, Change, Frontend, InvalidChangeRequest, LocalChange, MutableDocument, Path, Primitive,
    Value,
};
use serde_json::json;

pub struct Automerge {
    backend: Backend,
    frontend: Frontend,
    stream: Vec<Change>,
}

impl Automerge {
    pub fn new() -> Self {
        Self {
            backend: Backend::new(),
            frontend: Frontend::new(),
            stream: vec![],
        }
    }

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
            self.stream.push(change);
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
    // Let's say doc1 is the application state on device 1.
    // Further down we'll simulate a second device.
    // We initialize the document to initially contain an empty list of cards.
    let mut automerge = Automerge::new();
    automerge.change("Initial state", |doc| {
        doc.add_change(LocalChange::set(
            Path::root(),
            Value::from_json(&json!({ "cards": [] })),
        ))
    })?;

    // The doc1 object is treated as immutable -- you must never change it
    // directly. To change it, you need to call Automerge.change() with a callback
    // in which you can mutate the state. You can also include a human-readable
    // description of the change, like a commit message, which is stored in the
    // change history (see below).

    automerge.change("Add card", |doc| {
        doc.add_change(LocalChange::insert(
            Path::root().key("cards").index(0),
            Value::from_json(&json!({ "title": "Rewrite everything in Clojure", "done": false })),
        ))
    })?;

    // Now the state of doc1 is:
    // { cards: [ { title: 'Rewrite everything in Clojure', done: false } ] }

    // Automerge also defines an insertAt() method for inserting a new element at
    // a particular position in a list. Or you could use splice(), if you prefer.
    automerge.change("Add another card", |doc| {
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

    let mut automerge2 = Automerge::new();

    automerge2.apply_changes(automerge.changes())?;

    // Now make a change on device 1:
    automerge.change("Mark card as done", |doc| {
        doc.add_change(LocalChange::set(
            Path::root().key("cards").index(0).key("done"),
            Value::Primitive(Primitive::Boolean(true)),
        ))
    })?;

    // { cards:
    //    [ { title: 'Rewrite everything in Haskell', done: true },
    //      { title: 'Rewrite everything in Clojure', done: false } ] }

    // And, unbeknownst to device 1, also make a change on device 2:
    automerge2.change("Delete card", |doc| {
        doc.add_change(LocalChange::delete(Path::root().key("cards").index(1)))
    })?;

    // { cards: [ { title: 'Rewrite everything in Haskell', done: false } ] }

    // Now comes the moment of truth. Let's merge the changes from device 2 back
    // into device 1. You can also do the merge the other way round, and you'll get
    // the same result. The merged result remembers that 'Rewrite everything in
    // Haskell' was set to true, and that 'Rewrite everything in Clojure' was
    // deleted:

    automerge.apply_changes(automerge2.changes())?;

    // { cards: [ { title: 'Rewrite everything in Haskell', done: true } ] }
    println!("{}", automerge.state().to_json());

    // As our final trick, we can inspect the change history. Automerge
    // automatically keeps track of every change, along with the "commit message"
    // that you passed to change(). When you query that history, it includes both
    // changes you made locally, and also changes that came from other devices. You
    // can also see a snapshot of the application state at any moment in time in the
    // past. For example, we can count how many cards there were at each point:

    //Automerge.getHistory(finalDoc).map(state => [state.change.message, state.snapshot.cards.length])
    // [ [ 'Initialization', 0 ],
    //   [ 'Add card', 1 ],
    //   [ 'Add another card', 2 ],
    //   [ 'Mark card as done', 2 ],
    //   [ 'Delete card', 1 ] ]

    Ok(())
}
