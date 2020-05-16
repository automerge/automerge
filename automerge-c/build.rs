extern crate cbindgen;

use std::env;
use std::path::PathBuf;

fn main() {
    let crate_dir = PathBuf::from(
        env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR env var is not defined"),
    );

    let config = cbindgen::Config::from_file("cbindgen.toml")
        .expect("Unable to find cbindgen.toml configuration file");

    if let Ok(writer) = cbindgen::generate_with_config(&crate_dir, config) {
        writer.write_to_file(crate_dir.join("automerge.h"));
    }
}
