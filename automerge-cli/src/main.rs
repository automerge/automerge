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
        changes_file: Option<PathBuf>,
    },

    Import {
        /// Format for input: json, toml
        #[clap(long, short, default_value = "json")]
        format: ExportFormat,

        /// Path to write Automerge changes to
        #[clap(parse(from_os_str), long, short)]
        out_file: Option<PathBuf>,
    },
}

fn open_file_or_stdin(maybe_path: Option<PathBuf>) -> Result<Box<dyn std::io::Read>> {
    if atty::is(atty::Stream::Stdin) {
        if let Some(path) = maybe_path {
            Ok(Box::new(File::open(&path).unwrap()))
        } else {
            Err(anyhow!(
                "Must provide file path if not providing input via stdin"
            ))
        }
    } else {
        Ok(Box::new(std::io::stdin()))
    }
}

fn create_file_or_stdout(maybe_path: Option<PathBuf>) -> Result<Box<dyn std::io::Write>> {
    if atty::is(atty::Stream::Stdout) {
        if let Some(path) = maybe_path {
            Ok(Box::new(File::create(&path).unwrap()))
        } else {
            Err(anyhow!("Must provide file path if not piping to stdout"))
        }
    } else {
        Ok(Box::new(std::io::stdout()))
    }
}

fn main() -> Result<()> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Export {
            changes_file,
            format,
        } => match format {
            ExportFormat::JSON => {
                let mut in_buffer = open_file_or_stdin(changes_file)?;
                export::export_json(&mut in_buffer, &mut std::io::stdout())
            }
            ExportFormat::TOML => unimplemented!(),
        },

        Command::Import { format, out_file } => match format {
            ExportFormat::JSON => {
                let mut out_buffer = create_file_or_stdout(out_file)?;
                import::import_json(std::io::stdin(), &mut out_buffer)
            }
            ExportFormat::TOML => unimplemented!(),
        },
    }
}
