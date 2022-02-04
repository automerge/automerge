extern crate cbindgen;

use std::{env, path::PathBuf};

fn main() {
    let crate_dir = PathBuf::from(
        env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR env var is not defined"),
    );

    let config = cbindgen::Config::from_file("cbindgen.toml")
        .expect("Unable to find cbindgen.toml configuration file");

    if let Ok(writer) = cbindgen::generate_with_config(&crate_dir, config) {
        writer.write_to_file(crate_dir.join("automerge.h"));

        // Also write the generated header into the target directory when
        // specified (necessary for an out-of-source build a la CMake).
        if let Ok(target_dir) = env::var("CARGO_TARGET_DIR") {
            writer.write_to_file(PathBuf::from(target_dir).join("automerge.h"));
        }
    }
}
