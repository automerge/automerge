const am = require("@automerge/automerge/slim")
const wasmBlob = require("@automerge/automerge/automerge.wasm")

function component() {
  let initialized = false
  am.wasmInitialized().then(() => {
    initialized = true
  })

  am.initializeWasm(wasmBlob).then(() => {
    const element = document.createElement('div');
    element.id = "result"
    const doc = am.from({message: "hello automerge"})

    setTimeout(() => {
      if (!initialized) {
        throw new Error("wasm not initialized")
      }
      element.innerHTML = doc.message
      document.body.appendChild(element);
    }, 100)
  }).catch(e => {
    console.log("error initializing: ", e)
  })
}

component()

