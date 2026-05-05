import { execFileSync } from "child_process"
import fs from "fs"
import path from "path"

const target = process.env.TARGET
const profile = process.env.PROFILE ?? "dev"
const targetDir = process.env.TARGET_DIR ?? profile
const command = process.argv[2] ?? "target"

if (!target && command !== "compile") {
  throw new Error("TARGET must be set before running target.mjs")
}

if (command === "target") {
  fs.rmSync(target, { recursive: true, force: true })
}

if (command === "target" || command === "compile") {
  execFileSync(
    "cargo",
    ["build", "--target", "wasm32-unknown-unknown", "--profile", profile],
    {
      stdio: "inherit",
    },
  )
}

if (command === "target" || command === "bindgen") {
  execFileSync(
    "wasm-bindgen",
    [
      "--omit-default-module-path",
      "--weak-refs",
      "--typescript",
      "--target",
      target,
      "--out-dir",
      target,
      path.join(
        "..",
        "target",
        "wasm32-unknown-unknown",
        targetDir,
        "automerge_wasm.wasm",
      ),
    ],
    { stdio: "inherit" },
  )
}

if (command === "target" || command === "opt") {
  execFileSync(process.execPath, ["./opt-wasm.mjs"], {
    env: process.env,
    stdio: "inherit",
  })
}
