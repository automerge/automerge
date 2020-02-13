# Automerge

[![docs](https://docs.rs/automerge/badge.svg)](https://docs.rs/automerge)
[![crates](https://crates.io/crates/automerge)](https://crates.io/crates/automerge)
[![Build Status](https://travis-ci.org/alexjg/automerge-rs.svg?branch=master)](https://travis-ci.org/alexjg/automerge-rs)


This is a very early, very much work in progress implementation of [automerge](https://github.com/automerge/automerge) in rust. At the moment it implements a simple interface for reading the state of an OpSet, and a really horrendous interface for generating new changes to the Opset. 

## Plans

We're tentatively working on a plan to write a backend for the current javascript implementation of Automerge in Rust. The javascript Automerge library is split into two parts, a "frontend" and a "backend". The "backend" contains a lot of the more complex logic of the CRDT and also has a fairly small API. Given these facts we think we might be able to write a rust implementation of the backend, which compiles to WASM and can be used as a drop in replacement for the current backend. This same rust implementation could also be used via FFI on a lot of other platforms, which would make language interop much easier. This is all early days but it's very exciting.

For now though, it's a mostly broken pure rust implementation

## How to use

Add this to your dependencies

```
automerge = "0.0.2"
```

You'll need to export changes from automerge as JSON rather than using the encoding that `Automerge.save` uses. So first do this (in javascript):

```javascript
const doc = <your automerge document>
const changes = Automerge.getHistory(doc).map(h => h.change)
console.log(JSON.stringify(changes, null, 4))
```

Now you can load these changes into automerge like so:


```rust,no_run
extern crate automerge;

fn main() {
    let changes: Vec<automerge::Change> = serde_json::from_str("<paste the changes JSON here>").unwrap();
    let document = automerge::Document::load(changes).unwrap();
    let state: serde_json::Value = document.state().unwrap();
    println!("{:?}", state);
}
```

You can create new changes to the document by doing things like this:

```rust,no_run
extern crate automerge;

fn main() {
    let mut doc = Document::init();
    let json_value: serde_json::Value = serde_json::from_str(
        r#"
        {
            "cards_by_id": {},
            "size_of_cards": 12.0,
            "numRounds": 11.0,
            "cards": [1.0, false]
        }
    "#,
    )
    .unwrap();
    doc.create_and_apply_change(
        Some("Some change".to_string()),
        vec![ChangeRequest::Set {
            path: Path::root().key("the-state".to_string()),
            value: Value::from_json(&json_value),
        }],
    )
    .unwrap();
}
```

Check the docs on `ChangeRequest` for more information on what you can do.
