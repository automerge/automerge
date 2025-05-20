import  * as am from "@automerge/automerge/slim"
import wasmBlob from "@automerge/automerge/automerge.wasm?url"

function component() {
  let initialized = false
  am.wasmInitialized().then(() => {
    initialized = true
  })

  am.initializeWasm(fetch(wasmBlob)).then(() => {
    const element = document.createElement('div');
    element.id = "result"
    const doc = am.from({message: "hello automerge"})

    setTimeout(() => {
      if (!initialized) {
        throw new Error("wasm not initialized")
      }
      const element = document.createElement('div');
      element.id = "result"

      element.innerHTML = doc.message
      document.body.appendChild(element);
    }, 100)
  })
}

component()
