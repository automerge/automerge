use automerge::sync;
use automerge::{Automerge, AutomergeError};
use std::fs;
use std::time::Instant;

fn main() -> Result<(), AutomergeError> {
    let contents = fs::read("badmessage").expect("cant read badmessage file");
    let mut doc = Automerge::new();
    let mut state = sync::State::new();
    let now = Instant::now();
    // decode and receive happen at the same time in wasm so lets keep it apples to apples
    let message = sync::Message::decode(contents.as_slice()).expect("cant decode message");
    doc.receive_sync_message(&mut state, message).unwrap();
    println!("decode/receive   in {} ms", now.elapsed().as_millis());
    let now = Instant::now();
    let saved = doc.save();
    println!("save in             {} ms", now.elapsed().as_millis());
    let now = Instant::now();
    let _ = Automerge::load(&saved).unwrap();
    println!("load in             {} ms", now.elapsed().as_millis());
    let mut doc2 = Automerge::new();
    doc2.load_incremental(saved.as_slice()).unwrap();
    println!("load_incremental in {} ms", now.elapsed().as_millis());
    Ok(())
}
