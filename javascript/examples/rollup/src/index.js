import * as Automerge from "@automerge/automerge"

let doc = Automerge.init()
doc = Automerge.change(doc, d => (d.hello = "from automerge"))
const result = JSON.stringify(doc)

const element = document.createElement("div")
element.innerHTML = JSON.stringify(result)
document.body.appendChild(element)
