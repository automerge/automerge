import * as Automerge from "@automerge/automerge"

// hello world code that will run correctly on web or node

let doc = Automerge.init()
doc = Automerge.change(doc, (d: any) => (d.hello = "from automerge"))
const result = JSON.stringify(doc)

if (typeof document !== "undefined") {
  // browser
  const element = document.createElement("div")
  element.innerHTML = JSON.stringify(result)
  document.body.appendChild(element)
} else {
  // server
  console.log("node:", result)
}
