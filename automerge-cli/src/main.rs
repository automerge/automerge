use std::{fs::File, path::PathBuf, str::FromStr};

use anyhow::{anyhow, Result};
use clap::Clap;

mod change;
mod examine;
mod export;
mod import;
mod merge;

#[derive(Debug, Clap)]
#[clap(about = "Automerge CLI")]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug)]
enum ExportFormat {
    Json,
    Toml,
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

#[derive(Debug, Clap)]
enum Command {
    /// Output current state of an Automerge document in a specified format
    Export {
        /// Format for output: json, toml
        #[clap(long, short, default_value = "json")]
        format: ExportFormat,

        /// Path that contains Automerge changes
        #[clap(parse(from_os_str))]
        changes_file: Option<PathBuf>,

        /// The file to write to. If omitted assumes stdout
        #[clap(parse(from_os_str), long("out"), short('o'))]
        output_file: Option<PathBuf>,
    },

    Import {
        /// Format for input: json, toml
        #[clap(long, short, default_value = "json")]
        format: ExportFormat,

        #[clap(parse(from_os_str))]
        input_file: Option<PathBuf>,

        /// Path to write Automerge changes to
        #[clap(parse(from_os_str), long("out"), short('o'))]
        changes_file: Option<PathBuf>,
    },

    /// Read an automerge document from a file or stdin, perform a change on it and write a new
    /// document to stdout or the specified output file.
    Change {
        /// The change script to perform. Change scripts have the form <command> <path> [<JSON value>].
        /// The possible commands are 'set', 'insert', 'delete', and 'increment'.
        ///
        /// Paths look like this: $["mapkey"][0]. They always lways start with a '$', then each
        /// subsequent segment of the path is either a string in double quotes to index a key in a
        /// map, or an integer index to address an array element.
        ///
        /// Examples
        ///
        /// ## set
        ///
        /// > automerge change 'set $["someobject"] {"items": []}' somefile
        ///
        /// ## insert
        ///
        /// > automerge change 'insert $["someobject"]["items"][0] "item1"' somefile
        ///
        /// ## increment
        ///
        /// > automerge change 'increment $["mycounter"]'
        ///
        /// ## delete
        ///
        /// > automerge change 'delete $["someobject"]["items"]' somefile
        script: String,

        /// The file to change, if omitted will assume stdin
        #[clap(parse(from_os_str))]
        input_file: Option<PathBuf>,

        /// Path to write Automerge changes to, if omitted will write to stdout
        #[clap(parse(from_os_str), long("out"), short('o'))]
        output_file: Option<PathBuf>,
    },

    /// Read an automerge document and print a JSON representation of the changes in it to stdout
    Examine { input_file: Option<PathBuf> },

    /// Read one or more automerge documents and output a merged, compacted version of them
    Merge {
        /// The file to write to. If omitted assumes stdout
        #[clap(parse(from_os_str), long("out"), short('o'))]
        output_file: Option<PathBuf>,
        /// The file(s) to compact. If empty assumes stdin
        input: Vec<PathBuf>,
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
    tracing_subscriber::fmt::init();
    let opts = Opts::parse();
    match opts.cmd {
        Command::Export {
            changes_file,
            format,
            output_file,
        } => {
            let output: Box<dyn std::io::Write> = if let Some(output_file) = output_file {
                Box::new(File::create(&output_file)?)
            } else {
                Box::new(std::io::stdout())
            };
            match format {
                ExportFormat::Json => {
                    let mut in_buffer = open_file_or_stdin(changes_file)?;
                    export::export_json(&mut in_buffer, output, atty::is(atty::Stream::Stdout))
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
        Command::Change {
            input_file,
            output_file,
            script,
        } => {
            let in_buffer = open_file_or_stdin(input_file)?;
            let mut out_buffer = create_file_or_stdout(output_file)?;
            change::change(in_buffer, &mut out_buffer, script.as_str())
                .map_err(|e| anyhow::format_err!("Unable to make changes: {:?}", e))
        }
        Command::Examine { input_file } => {
            let in_buffer = open_file_or_stdin(input_file)?;
            let out_buffer = std::io::stdout();
            match examine::examine(in_buffer, out_buffer, atty::is(atty::Stream::Stdout)) {
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
