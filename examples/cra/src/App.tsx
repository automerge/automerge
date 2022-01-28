import React, { useEffect, useState } from 'react';
import logo from './logo.svg';
import './App.css';
import init from "automerge-wasm"
import { create, loadDoc, encodeChange, decodeChange,
         initSyncState, encodeSyncState, decodeSyncState,
         encodeSyncMessage, decodeSyncMessage,
         LIST, MAP, TEXT } from "automerge-wasm"

function App() {
  const [ val, setVal ] = useState("");
  useEffect(() => {
    init().then(() => {
      let doc = create()
      let edits = doc.set("_root", "edits", TEXT) || ""
      doc.splice(edits, 0, 0, "the quick fox jumps over the lazy dog")
      doc.splice(edits, 10, 3, "sloth")
      let result = doc.text(edits)
      setVal(JSON.stringify(result))
    })
  }, [])
  return (
    <div className="App">
      <header className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <p>
          Edit <code>src/App.tsx</code> and save to reload.
        </p>
        <a
          className="App-link"
          href="https://reactjs.org"
          target="_blank"
          rel="noopener noreferrer"
        >
          Learn React
        </a>
        <p> edits = {val}</p>
      </header>
    </div>
  );
}

export default App;
