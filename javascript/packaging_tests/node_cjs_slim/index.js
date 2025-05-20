const am = require("@automerge/automerge");
const fs = require("fs")

let initialized = false
am.wasmInitialized().then(() => {
  initialized = true
})

const wasmBlob = fs.readFileSync("./node_modules/@automerge/automerge/dist/automerge.wasm")
am.initializeWasm(wasmBlob).then(() => {
  const doc = am.from({message: "hello webpack"})
  console.log(doc.message)
})

setTimeout(() => {
  if (!initialized) {
    console.error("wasm not initialized")
    process.exit(1)
  }
}, 100)
