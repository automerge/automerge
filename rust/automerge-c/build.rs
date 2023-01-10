extern crate cbindgen;

use std::{env, path::PathBuf};

fn main() {
    let crate_dir = PathBuf::from(
        env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR env var is not defined"),
    );

    let config = cbindgen::Config::from_file("cbindgen.toml")
        .expect("Unable to find cbindgen.toml configuration file");

    if let Ok(writer) = cbindgen::generate_with_config(crate_dir, config) {
        // \note CMake sets this environment variable before invoking Cargo so
        //       that it can direct the generated header file into its
        //       out-of-source build directory for post-processing.
        if let Ok(target_dir) = env::var("CBINDGEN_TARGET_DIR") {
            writer.write_to_file(PathBuf::from(target_dir).join("automerge.h"));
        }
    }
}
