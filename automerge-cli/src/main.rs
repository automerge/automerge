extern crate automerge_backend;

use automerge_backend::{Backend};
use std::fs::File;
use std::io::prelude::*;

fn main() -> std::io::Result<()> {
    let args : Vec<_> = std::env::args().collect();
    if args.len() != 2 {
        println!("usage: automerge PATH");
        return Ok(());
    }
    let data : Vec<u8> = std::fs::read(&args[1])?;
    println!("DATA: {:?}",data.len());
    let mut db = Backend::init();
    db.load_changes_binary(vec![data]).unwrap();
    return Ok(());
}
