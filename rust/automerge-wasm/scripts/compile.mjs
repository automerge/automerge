// Runs `cargo build` for the wasm target with the panic=unwind strategy so
// that Rust panics are caught at the JS boundary by wasm-bindgen and thrown as
// `PanicError` exceptions instead of aborting the WASM module.
//
// This requires the nightly toolchain (for -Zbuild-std) and the rust-src
// rustup component. The nightly toolchain is selected here via rustup's
// "+toolchain" argument so the workspace's pinned stable toolchain (see
// rust/rust-toolchain.toml) is left untouched for everything else.
//
// Current nightlies emit modern (exnref) Wasm exception handling by default.
// We force legacy EH below so wasm-bindgen can provide its WebAssembly.JSTag
// polyfill for runtimes without a native JSTag implementation.

import { spawnSync } from "node:child_process"

const profile = process.env.PROFILE ?? "dev"
const toolchain = process.env.WASM_TOOLCHAIN ?? "nightly"
// `cargo +<toolchain>` requires rustup. Nix supplies a cargo executable which
// already selects the pinned nightly toolchain via WASM_CARGO.
const cargo = process.env.WASM_CARGO ?? "cargo"

const args = [
  ...(process.env.WASM_CARGO ? [] : [`+${toolchain}`]),
  "build",
  "--target",
  "wasm32-unknown-unknown",
  "--profile",
  profile,
  // Rebuild std with the unwind panic runtime (only available on nightly).
  "-Zbuild-std=std,panic_unwind",
]

// Compose RUSTFLAGS, preserving any the user already set.
const env = { ...process.env }
const extra = "-C panic=unwind -C llvm-args=-wasm-use-legacy-eh"
env.RUSTFLAGS = env.RUSTFLAGS ? `${env.RUSTFLAGS} ${extra}` : extra

const result = spawnSync(cargo, args, { stdio: "inherit", env })
if (result.error) {
  console.error(result.error)
  process.exit(1)
}
process.exit(result.status ?? 1)
