use std::{fs::File, io::IsTerminal, path::PathBuf, str::FromStr};

use anyhow::{anyhow, Result};
use clap::{
    builder::{BoolishValueParser, TypedValueParser, ValueParserFactory},
    Parser,
};

mod color_json;
mod examine;
mod examine_sync;
mod export;
mod import;
mod merge;

#[derive(Parser, Debug)]
#[clap(about = "Automerge CLI")]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum ExportFormat {
    Json,
    Toml,
}

#[derive(Copy, Clone, Default, Debug)]
pub(crate) struct VerifyFlag(bool);

impl VerifyFlag {
    fn load(&self, buf: &[u8]) -> Result<automerge::Automerge, automerge::AutomergeError> {
        if self.0 {
            automerge::Automerge::load(buf)
        } else {
            automerge::Automerge::load_unverified_heads(buf)
        }
    }
}

#[derive(Clone)]
struct VerifyFlagParser;
impl ValueParserFactory for VerifyFlag {
    type Parser = VerifyFlagParser;

    fn value_parser() -> Self::Parser {
        VerifyFlagParser
    }
}

impl TypedValueParser for VerifyFlagParser {
    type Value = VerifyFlag;

    fn parse_ref(
        &self,
        cmd: &clap::Command,
        arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, clap::Error> {
        BoolishValueParser::new()
            .parse_ref(cmd, arg, value)
            .map(VerifyFlag)
    }
}

impl FromStr for ExportFormat {
    type Err = anyhow::Error;

    fn from_str(input: &str) -> Result<ExportFormat> {
        match input {
            "json" => Ok(ExportFormat::Json),
            "toml" => Ok(ExportFormat::Toml),
            _ => Err(anyhow!("Invalid export format: {}", input)),
        }
    }
}

#[derive(Debug, Parser)]
enum Command {
    /// Output current state of an Automerge document in a specified format
    Export {
        /// Format for output: json, toml
        #[clap(long, short, default_value = "json")]
        format: ExportFormat,

        /// Path that contains Automerge changes
        changes_file: Option<PathBuf>,

        /// The file to write to. If omitted assumes stdout
        #[clap(long("out"), short('o'))]
        output_file: Option<PathBuf>,

        /// Whether to verify the head hashes of a compressed document
        #[clap(long, action = clap::ArgAction::SetFalse)]
        skip_verifying_heads: VerifyFlag,
    },

    Import {
        /// Format for input: json, toml
        #[clap(long, short, default_value = "json")]
        format: ExportFormat,

        input_file: Option<PathBuf>,

        /// Path to write Automerge changes to
        #[clap(long("out"), short('o'))]
        changes_file: Option<PathBuf>,
    },

    /// Read an automerge document and print a JSON representation of the changes in it to stdout
    Examine {
        input_file: Option<PathBuf>,

        /// Whether to verify the head hashes of a compressed document
        #[clap(long, action = clap::ArgAction::SetFalse)]
        skip_verifying_heads: VerifyFlag,
    },

    /// Read an automerge sync messaage and print a JSON representation of it
    ExamineSync { input_file: Option<PathBuf> },

    /// Read one or more automerge documents and output a merged, compacted version of them
    Merge {
        /// The file to write to. If omitted assumes stdout
        #[clap(long("out"), short('o'))]
        output_file: Option<PathBuf>,

        /// The file(s) to compact. If empty assumes stdin
        input: Vec<PathBuf>,
    },
}

fn open_file_or_stdin(maybe_path: Option<PathBuf>) -> Result<Box<dyn std::io::Read>> {
    if std::io::stdin().is_terminal() {
        if let Some(path) = maybe_path {
            Ok(Box::new(File::open(path).unwrap()))
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
    if std::io::stdout().is_terminal() {
        if let Some(path) = maybe_path {
            Ok(Box::new(File::create(path).unwrap()))
        } else {
            Err(anyhow!("Must provide file path if not piping to stdout"))
        }
    } else {
        Ok(Box::new(std::io::stdout()))
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let opts = Opts::parse();
    match opts.cmd {
        Command::Export {
            changes_file,
            format,
            output_file,
            skip_verifying_heads,
        } => {
            let output: Box<dyn std::io::Write> = if let Some(output_file) = output_file {
                Box::new(File::create(output_file)?)
            } else {
                Box::new(std::io::stdout())
            };
            match format {
                ExportFormat::Json => {
                    let mut in_buffer = open_file_or_stdin(changes_file)?;
                    export::export_json(
                        &mut in_buffer,
                        output,
                        skip_verifying_heads,
                        std::io::stdout().is_terminal(),
                    )
                }
                ExportFormat::Toml => unimplemented!(),
            }
        }
        Command::Import {
            format,
            input_file,
            changes_file,
        } => match format {
            ExportFormat::Json => {
                let mut out_buffer = create_file_or_stdout(changes_file)?;
                let mut in_buffer = open_file_or_stdin(input_file)?;
                import::import_json(&mut in_buffer, &mut out_buffer)
            }
            ExportFormat::Toml => unimplemented!(),
        },
        Command::Examine {
            input_file,
            skip_verifying_heads,
        } => {
            let in_buffer = open_file_or_stdin(input_file)?;
            let out_buffer = std::io::stdout();
            match examine::examine(
                in_buffer,
                out_buffer,
                skip_verifying_heads,
                std::io::stdout().is_terminal(),
            ) {
                Ok(()) => {}
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                }
            }
            Ok(())
        }
        Command::ExamineSync { input_file } => {
            let in_buffer = open_file_or_stdin(input_file)?;
            let out_buffer = std::io::stdout();
            match examine_sync::examine_sync(in_buffer, out_buffer, std::io::stdout().is_terminal())
            {
                Ok(()) => {}
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                }
            }
            Ok(())
        }
        Command::Merge { input, output_file } => {
            let out_buffer = create_file_or_stdout(output_file)?;
            match merge::merge(input.into(), out_buffer) {
                Ok(()) => {}
                Err(e) => {
                    eprintln!("Failed to merge: {}", e);
                }
            };
            Ok(())
        }
    }
}
