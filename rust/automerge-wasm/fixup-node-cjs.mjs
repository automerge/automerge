import fs from "fs"

fs.cpSync("index.d.ts", "nodejs/automerge_wasm.d.cts")
fs.renameSync("nodejs/automerge_wasm.js", "nodejs/automerge_wasm.cjs")
