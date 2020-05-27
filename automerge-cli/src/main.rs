use anyhow::{anyhow, Result};
use clap::Clap;
use std::fs::File;
use std::path::PathBuf;
use std::str::FromStr;

mod export;
mod import;

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
    type Err = anyhow::Error;

    fn from_str(input: &str) -> Result<ExportFormat> {
        match input {
            "json" => Ok(ExportFormat::JSON),
            "toml" => Ok(ExportFormat::TOML),
            _ => Err(anyhow!("Invalid export format: {}", input)),
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
        #[clap(parse(from_os_str))]
        changes_file: PathBuf,
    },

    Import {
        /// Format for input: json, toml
        #[clap(long, short, default_value = "json")]
        format: ExportFormat,

        /// Path to write Automerge changes to
        // TODO: How to conditionally require outfile based on isatty?
        #[clap(long, short)]
        out_file: String,
    },
}

fn main() -> Result<()> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Export {
            changes_file,
            format,
        } => match format {
            ExportFormat::JSON => {
                let mut f = File::open(&changes_file)?;
                export::export_json(&mut f, &mut std::io::stdout())
            }
            ExportFormat::TOML => unimplemented!(),
        },

        Command::Import { format, out_file } => match format {
            // TODO: import_json returns a String, how do we pipe this correctly
            // either to a file or to stdout?
            ExportFormat::JSON => {
                let mut buffer = File::create(out_file)?;
                import::import_json(std::io::stdin(), &mut buffer)
            }
            ExportFormat::TOML => unimplemented!(),
        },
    }
}
