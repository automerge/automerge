pub(crate) fn repair(
    mut input: impl std::io::Read,
    mut output: impl std::io::Write,
    _is_tty: bool,
) {
    let mut buf: Vec<u8> = Vec::new();
    match input.read_to_end(&mut buf) {
        Ok(_) => {}
        Err(e) => {
            eprintln!("Unable to read input: {:?}", e);
            return;
        }
    };
    if automerge::Automerge::load(&buf).is_ok() {
        eprintln!("No repair needed");
        return;
    }
    match automerge::Automerge::repair(&buf) {
        Ok(Some(mut doc)) => {
            eprintln!("success!");
            output.write_all(&doc.save()).unwrap();
        }
        Ok(None) => {
            eprintln!("unable to repair");
        }
        Err(e) => {
            eprintln!("Unable to repair: {:?}", e);
        }
    }
}
