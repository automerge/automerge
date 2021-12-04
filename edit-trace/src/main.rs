//mod sequence_tree;

use std::fs;
use json;
use std::time::Instant;
use automerge::{ Automerge, ROOT, Value, AutomergeError };
use rand::prelude::*;
//use sequence_tree::SequenceTree;

fn main() -> Result<(), AutomergeError> {
/*
  let mut rng = rand::thread_rng();
  let mut t = SequenceTree::new();
  for i in 0..100000 {
    let j : usize = rng.gen();
    let j = j % (t.len() + 1);
    t.insert(j ,i)
  }
  Ok(())
*/
  let contents = fs::read_to_string("edits.json").expect("cannot read edits file");
  let edits = json::parse(&contents).expect("cant parse edits");
  let mut commands = vec![];
  for i in 0..edits.len() {
    let pos : usize = edits[i][0].as_usize().unwrap();
    let del : usize = edits[i][1].as_usize().unwrap();
    let mut vals = vec![];
    for j in 2..edits[i].len() {
      let v = edits[i][j].as_str().unwrap();
      vals.push(Value::str(v));
    }
    commands.push((pos,del,vals));
  }
  let mut doc = Automerge::new();

  let now = Instant::now();
  doc.begin()?;
  let text = doc.set(&ROOT, "text".into(), Value::text()).unwrap();
  for (i,(pos,del,vals)) in commands.into_iter().enumerate() {
    if i % 1000 == 0 {
        println!("Processed {} edits in {} ms",i,now.elapsed().as_millis());
    }
    doc.splice(&text.into(), pos, del, vals)?;
    //doc.splice(&text.into(), 0, del, vals)?;
  }
  doc.commit()?;
  Ok(())
}
