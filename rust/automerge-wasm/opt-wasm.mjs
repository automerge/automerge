import { execFileSync } from "child_process"
import fs from "fs"
import path from "path"

const target = process.env.TARGET
const profile = process.env.PROFILE ?? "dev"

if (!target) {
  throw new Error("TARGET must be set before running opt-wasm.mjs")
}

const wasmPath = path.join(target, "automerge_wasm_bg.wasm")
if (!fs.existsSync(wasmPath)) {
  throw new Error(`WASM artifact not found: ${wasmPath}`)
}

try {
  execFileSync("wasm-opt", ["--version"], { stdio: "ignore" })
} catch (error) {
  if (profile === "dev") {
    console.warn("wasm-opt not found; skipping dev WASM optimization")
    process.exit(0)
  }
  throw new Error("wasm-opt is required for release WASM builds")
}

execFileSync(
  "wasm-opt",
  [
    "-Oz",
    "--converge",
    "--strip-debug",
    "--strip-dwarf",
    "--strip-producers",
    "--enable-bulk-memory",
    "--enable-nontrapping-float-to-int",
    wasmPath,
    "-o",
    wasmPath,
  ],
  {
    stdio: "inherit",
  },
)
