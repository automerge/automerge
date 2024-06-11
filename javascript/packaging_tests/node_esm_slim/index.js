import {next as am} from "@automerge/automerge/slim"
import fs from "fs"

let initialized = false
am.wasmInitialized().then(() => {
  initialized = true
})

const wasm = fs.readFileSync("./node_modules/@automerge/automerge/dist/wasm_blob.wasm")
await am.initializeWasm(wasm)

const doc = am.from({message: "hello webpack"})
console.log(doc.message)

setTimeout(() => {
  if (!initialized) {
    console.error("wasm not initialized")
    process.exit(1)
  }
}, 100)

