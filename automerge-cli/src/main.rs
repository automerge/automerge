use std::path::Path;
use std::str::FromStr;
use clap::Clap;

mod error;
mod export;

#[derive(Debug, Clap)]
#[clap(about = "Automerge CLI")]
struct Opts {
    #[clap(subcommand)]
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

#[derive(Debug, Clap)]
enum Command {
    /// Output current state of an Automerge document in a specified format
    Export {
        /// Format for output: json, toml
        #[clap(long, short, default_value = "json")]
        format: ExportFormat,

        /// File that contains automerge changes
        changes_file: String,
    },
}

fn main() -> Result<(), error::AutomergeCliError> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Export {
            changes_file,
            format,
        } => match format {
            ExportFormat::JSON => export::export_json(Path::new(&changes_file)),
            ExportFormat::TOML => unimplemented!(),
        },
    }
}
