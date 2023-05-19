
use std::fs;
use clap::Parser;
use automerge::{ repair };//AutomergeError, Change, Prop, ReadDoc, ExpandedKey, ExpandedOpId, ExpandedOpType, Automerge };

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// file to repair
    #[arg(short, long)]
    file: String,
}

fn main() {
    let args = Args::parse();
    println!("Repairing file {}", args.file);

    let contents = fs::read(args.file).expect("Should have been able to read the file");

    println!("File {} bytes", contents.len());

    let _doc = repair(&contents).unwrap();
}
