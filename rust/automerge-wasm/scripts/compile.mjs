// Runs `cargo build` for the wasm target with the panic=unwind strategy so
// that Rust panics are caught at the JS boundary by wasm-bindgen and thrown as
// `PanicError` exceptions instead of aborting the WASM module.
//
// This requires the nightly toolchain (for -Zbuild-std) and the rust-src
// rustup component. The nightly toolchain is selected here via rustup's
// "+toolchain" argument so the workspace's pinned stable toolchain (see
// rust/rust-toolchain.toml) is left untouched for everything else.
//
// CI overrides WASM_TOOLCHAIN with a pinned dated nightly (e.g.
// `nightly-2026-04-25`) for reproducible builds. Local dev defaults to
// `nightly`.

import { spawnSync } from "node:child_process"

const profile = process.env.PROFILE ?? "dev"
const toolchain = process.env.WASM_TOOLCHAIN ?? "nightly"

const args = [
  `+${toolchain}`,
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
const extra = "-C panic=unwind"
env.RUSTFLAGS = env.RUSTFLAGS ? `${env.RUSTFLAGS} ${extra}` : extra

const result = spawnSync("cargo", args, { stdio: "inherit", env })
if (result.error) {
  console.error(result.error)
  process.exit(1)
}
process.exit(result.status ?? 1)
