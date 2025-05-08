import * as am from "@automerge/automerge"

const doc = am.from({message: "hello webpack"})
console.log(doc.message)

let initialized = false
am.wasmInitialized().then(() => {
  initialized = true
})

setTimeout(() => {
  if (!initialized) {
    console.error("wasm not initialized")
    process.exit(1)
  }
}, 100)
