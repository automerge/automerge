import init, { create } from "automerge-wasm"

// hello world code that will run correctly on web or node

init().then((Automerge) => {
  console.log("Automerge=", Automerge)
  console.log("create=", create)
  const doc = Automerge.create()
  doc.put("/", "hello", "world")
  const result = doc.materialize("/")
  //const result = xxx

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

