use automerge_frontend::{Frontend, Value, LocalChange, Path};
use automerge_protocol as amp;

#[test]
fn test_delete_index_in_mutation() {
    let mut frontend = Frontend::new();
    let _cr = frontend.change(None, |doc| {
        doc.add_change(LocalChange::set(
            Path::root().key("vals"),
            Value::Sequence(Vec::new(), amp::SequenceType::List),
        ))?;
        Ok(())
    }).unwrap();

    frontend.change(None, |doc| {
        doc.add_change(LocalChange::insert(
            Path::root().key("vals").index(0),
            Value::Primitive("0".into()),
        ))?;
        Ok(())
    }).unwrap();

    frontend.change(None, |doc| {
        doc.add_change(LocalChange::insert(
            Path::root().key("vals").index(1),
            Value::Primitive("1".into()),
        ))?;
        Ok(())
    }).unwrap();

    frontend.change(None, |doc| {
        doc.add_change(LocalChange::delete(
            Path::root().key("vals").index(1),
        ))?;
        Ok(())
    }).unwrap();

}
