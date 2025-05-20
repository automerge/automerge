const am = require("@automerge/automerge")

function component() {

  let initialized = false
  am.wasmInitialized().then(() => {
    initialized = true
  })

  setTimeout(() => {
    if (!initialized) {
      throw new Error("wasm not initialized")
    }
    const element = document.createElement('div');
    element.id = "result"
    const doc = am.from({message: "hello automerge"})

    element.innerHTML = doc.message
    document.body.appendChild(element);
  }, 100)
}

component()

