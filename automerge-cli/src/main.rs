use clap::Clap;
use std::{fs::File, io, io::Read};

#[derive(Debug, Clap)]
struct Opts {
    changes_file: String,
}

fn main() -> io::Result<()> {
    let opts = Opts::parse();
    let mut input_data = Vec::new();
    let mut input_file = File::open(opts.changes_file)?;
    input_file.read_to_end(&mut input_data)?;

    let mut backend = automerge_backend::Backend::init();
    backend.load_changes_binary(vec![input_data]).unwrap();
    let patch = backend.get_patch().unwrap();
    let mut frontend = automerge_frontend::Frontend::new();
    frontend.apply_patch(patch).unwrap();
    println!(
        "{}",
        serde_json::to_string_pretty(&frontend.state().to_json()).unwrap()
    );
    Ok(())
}
