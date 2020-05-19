use std::path::Path;
use structopt::StructOpt;

mod error;
mod export;

#[derive(Debug, StructOpt)]
#[structopt(about = "Automerge CLI")]
struct Opts {
    #[structopt(subcommand)]
    cmd: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    Export { changes_file: String },
}

fn main() -> Result<(), error::AutomergeCliError> {
    let opts = Opts::from_args();
    match opts.cmd {
        Command::Export { changes_file } => export::export_json(Path::new(&changes_file)),
    }
}
