// Runs `wasm-bindgen` on the blob `compile.mjs` built, for the target named
// by $TARGET, from the profile directory named by $TARGET_DIR.
//
// The blob's location comes from `cargo metadata` rather than assuming
// `../target`, so CARGO_TARGET_DIR and `build.target-dir` are honoured.

import { execFileSync, spawnSync } from "node:child_process";
import path from "node:path";

const target = process.env.TARGET;
const targetDir = process.env.TARGET_DIR;
if (!target || !targetDir) {
  console.error("TARGET and TARGET_DIR must be set");
  process.exit(1);
}

const cargo = process.env.WASM_CARGO ?? "cargo";
const { target_directory } = JSON.parse(
  execFileSync(cargo, ["metadata", "--format-version", "1", "--no-deps"], {
    encoding: "utf8",
  }),
);
const blob = path.join(
  target_directory,
  "wasm32-unknown-unknown",
  targetDir,
  "automerge_wasm.wasm",
);

const result = spawnSync(
  "wasm-bindgen",
  [
    "--omit-default-module-path",
    "--weak-refs",
    "--typescript",
    "--target",
    target,
    "--out-dir",
    target,
    blob,
  ],
  { stdio: "inherit" },
);
if (result.error) {
  console.error(result.error);
  process.exit(1);
}
process.exit(result.status ?? 1);
