import * as Automerge from "@automerge/automerge"
import logo from "./logo.svg"
import "./App.css"

let doc = Automerge.init()
doc = Automerge.change(doc, d => (d.hello = "from automerge"))
const result = JSON.stringify(doc)

function App() {
  return (
    <div className="App">
      <header className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <p>{result}</p>
      </header>
    </div>
  )
}

export default App
