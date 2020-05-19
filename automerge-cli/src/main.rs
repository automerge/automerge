use std::path::Path;
use std::str::FromStr;
use structopt::StructOpt;

mod error;
mod export;

#[derive(Debug, StructOpt)]
#[structopt(about = "Automerge CLI")]
struct Opts {
    #[structopt(subcommand)]
    cmd: Command,
}

#[derive(Debug)]
enum ExportFormat {
    JSON,
    TOML,
}

impl FromStr for ExportFormat {
    type Err = error::AutomergeCliError;

    fn from_str(input: &str) -> Result<ExportFormat, error::AutomergeCliError> {
        match input {
            "json" => Ok(ExportFormat::JSON),
            "toml" => Ok(ExportFormat::TOML),
            _ => Err(error::AutomergeCliError::InvalidCommand),
        }
    }
}

#[derive(Debug, StructOpt)]
enum Command {
    Export {
        #[structopt(long, short, default_value = "json")]
        format: ExportFormat,
        changes_file: String,
    },
}

fn main() -> Result<(), error::AutomergeCliError> {
    let opts = Opts::from_args();
    match opts.cmd {
        Command::Export {
            changes_file,
            format,
        } => match format {
            ExportFormat::JSON => export::export_json(Path::new(&changes_file)),
            ExportFormat::TOML => unimplemented!()
        },
    }
}
