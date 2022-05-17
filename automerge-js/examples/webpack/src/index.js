import init, * as Automerge from "automerge-js"

// hello world code that will run correctly on web or node

init().then(_ => {
  let doc = Automerge.init()
  doc = Automerge.change(doc, (d) => d.hello = "from automerge-js")
  const result = JSON.stringify(doc)

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

