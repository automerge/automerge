use automerge::ObjType;
use automerge::{ReadDoc, transaction::Transactable, AutoCommit, AutomergeError, ROOT};
use serde_json::Value;
use std::time::Instant;

fn main() -> Result<(), AutomergeError> {
    let contents = include_str!("../edits.json");
    let edits: Vec<Value> = serde_json::from_str(contents).expect("cant parse edits");
    //let edits = json::parse(contents).expect("cant parse edits");
    let mut commands = vec![];
    for edit in &edits {
        let pos: usize = edit.as_array().unwrap()[0].as_u64().unwrap() as usize;
        let del: isize = edit.as_array().unwrap()[1].as_i64().unwrap() as isize;
        let mut vals = String::new();
        for j in 2..edit.as_array().unwrap().len() {
            let v = edit.as_array().unwrap()[j].as_str().unwrap();
            vals.push_str(v);
        }
        commands.push((pos, del, vals));
    }
    let mut doc = AutoCommit::new();
    doc.update_diff_cursor();

    let now = Instant::now();
    //let mut tx = doc.transaction();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    for (i, (pos, del, vals)) in commands.into_iter().enumerate() {
        if i % 1000 == 0 {
            println!("Processed {} edits in {} ms", i, now.elapsed().as_millis());
        }
        doc.splice_text(&text, pos, del, &vals)?;
    }
    println!("Done in {} ms", now.elapsed().as_millis());
    let commit = Instant::now();
    doc.commit();

    println!("Commit in {} ms", commit.elapsed().as_millis());
    let observe = Instant::now();
    let _patches = doc.diff_incremental();
    println!("Patches in {} ms", observe.elapsed().as_millis());
    let save = Instant::now();
    let bytes = doc.save();
    println!("Saved in {} ms", save.elapsed().as_millis());

    let fork = Instant::now();
    let heads = doc.get_heads();
    let _other = doc.fork_at(&heads);
    println!("ForkAt in {} ms", fork.elapsed().as_millis());

    let load = Instant::now();
    let _ = AutoCommit::load(&bytes).unwrap();
    println!("Loaded in {} ms", load.elapsed().as_millis());

    let get_txt = Instant::now();
    doc.text(&text)?;
    println!("Text in {} ms", get_txt.elapsed().as_millis());

    Ok(())
}
