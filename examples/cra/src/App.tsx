import React, { useEffect, useState } from 'react';
import './App.css';
import * as Automerge from "automerge-wasm"


function App() {
  const [ doc, ] = useState(Automerge.create())
  const [ edits, ] = useState(doc.set("_root", "edits", Automerge.TEXT) || "")
  const [ val, setVal ] = useState("");
  useEffect(() => {
      doc.splice(edits, 0, 0, "the quick fox jumps over the lazy dog")
      let result = doc.text(edits)
      setVal(result)
  }, [])

  function updateTextarea(e: any) {
    e.preventDefault()
    let event: InputEvent = e.nativeEvent
    console.log(edits, e.target.selectionEnd)
    switch (event.inputType) {
      case 'insertText':
        //@ts-ignore
        doc.splice(edits, e.target.selectionEnd - 1, 0, e.nativeEvent.data)
        break;
      case 'deleteContentBackward':
        //@ts-ignore
        doc.splice(edits, e.target.selectionEnd, 1)
        break;
      case 'insertLineBreak':
        //@ts-ignore
        doc.splice(edits, e.target.selectionEnd - 1, '\n')
        break;
    }
    setVal(doc.text(edits))
  }
  return (
    <div className="App">
      <header className="App-header">
        <textarea value={val} onChange={updateTextarea}></textarea>
      </header>
    </div>
  );
}

export default App;
