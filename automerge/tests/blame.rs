use automerge::transaction::Transactable;
use automerge::{AutoCommit, AutomergeError, ROOT};

/*
mod helpers;
use helpers::{
    pretty_print, realize, realize_obj,
    RealizedObject,
};
*/

#[test]
fn simple_blame_text() -> Result<(), AutomergeError> {
    let mut doc = AutoCommit::new();
    let note = doc.set_object(&ROOT, "note", automerge::ObjType::Text)?;
    doc.splice_text(&note, 0, 0, "hello little world")?;
    let baseline = doc.get_heads();
    assert!(doc.text(&note).unwrap() == "hello little world");
    let mut doc2 = doc.fork();
    doc2.splice_text(&note, 5, 7, " big")?;
    let h2 = doc2.get_heads();
    assert!(doc2.text(&note)? == "hello big world");
    let mut doc3 = doc.fork();
    doc3.splice_text(&note, 0, 0, "Well, ")?;
    let h3 = doc3.get_heads();
    assert!(doc3.text(&note)? == "Well, hello little world");
    doc.merge(&mut doc2)?;
    doc.merge(&mut doc3)?;
    let text = doc.text(&note)?;
    assert!(text == "Well, hello big world");
    let cs = vec![h2, h3];
    let blame = doc.blame(&note, &baseline, &cs)?;
    assert!(&text[blame[0].add[0].clone()] == " big");
    assert!(blame[0].del[0] == (15, " little".to_owned()));
    //println!("{:?} == {:?}", blame[0].del[0] , (15, " little".to_owned()));
    assert!(&text[blame[1].add[0].clone()] == "Well, ");
    //println!("- ------- blame = {:?}", blame);
    Ok(())
}
