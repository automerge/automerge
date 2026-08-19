import fs from "fs"

fs.cpSync("nodejs-memory64/automerge_wasm.d.ts", "nodejs-memory64/automerge_wasm.d.cts")
fs.renameSync("nodejs-memory64/automerge_wasm.js", "nodejs-memory64/automerge_wasm.cjs")
