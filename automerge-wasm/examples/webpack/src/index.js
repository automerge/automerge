import init, { create } from "automerge-wasm"

// hello world code that will run correctly on web or node

init().then(_ => {
  const doc = create()
  doc.put("/", "hello", "world")
  const result = doc.materialize("/")

  if (typeof document !== 'undefined') {
    // browser
    const element = document.createElement('div');
    element.innerHTML = JSON.stringify(result)
    document.body.appendChild(element);
  } else {
    // server
    console.log("node:", result)
  }
})

