# Automerge

[![docs](https://docs.rs/automerge/badge.svg)](docs.rs/automerge)
[![crates](https://crates.io/crates/automerge)](https://crates.io/crates/automerge)
[![Build Status](https://travis-ci.org/alexjg/automerge-rs.svg?branch=master)](https://travis-ci.org/alexjg/automerge-rs)


This is a very early, very much work in progress implementation of [automerge](https://github.com/automerge/automerge) in rust. At the moment it barely implements a read only view of operations received, with very little testing that it works. Objectives for it are:

- Full read and write replication
- `no_std` support to make it easy to use in WASM environments
- Model based testing to ensure compatibility with the JS library


## How to use

Add this to your dependencies

```
automerge = 0.0.2
```

You'll need to export changes from automerge as JSON rather than using the encoding that `Automerge.save` uses. So first do this:

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
