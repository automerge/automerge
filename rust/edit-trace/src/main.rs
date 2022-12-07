use automerge::ObjType;
use automerge::{transaction::Transactable, Automerge, AutomergeError, ROOT};
use std::time::Instant;

fn main() -> Result<(), AutomergeError> {
    let contents = include_str!("../edits.json");
    let edits = json::parse(contents).expect("cant parse edits");
    let mut commands = vec![];
    for i in 0..edits.len() {
        let pos: usize = edits[i][0].as_usize().unwrap();
        let del: usize = edits[i][1].as_usize().unwrap();
        let mut vals = String::new();
        for j in 2..edits[i].len() {
            let v = edits[i][j].as_str().unwrap();
            vals.push_str(v);
        }
        commands.push((pos, del, vals));
    }
    let mut doc = Automerge::new();

    let now = Instant::now();
    let mut tx = doc.transaction();
    let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
    for (i, (pos, del, vals)) in commands.into_iter().enumerate() {
        if i % 1000 == 0 {
            println!("Processed {} edits in {} ms", i, now.elapsed().as_millis());
        }
        tx.splice_text(&text, pos, del, &vals)?;
    }
    tx.commit();
    println!("Done in {} ms", now.elapsed().as_millis());
    let save = Instant::now();
    let bytes = doc.save();
    println!("Saved in {} ms", save.elapsed().as_millis());

    let load = Instant::now();
    let _ = Automerge::load(&bytes).unwrap();
    println!("Loaded in {} ms", load.elapsed().as_millis());

    let get_txt = Instant::now();
    doc.text(&text)?;
    println!("Text in {} ms", get_txt.elapsed().as_millis());

    Ok(())
}
