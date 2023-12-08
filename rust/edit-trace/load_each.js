const fs = require('fs');
const util = require('util');
let files = [
/*
  './slow.automerge',
  './slow-font.automerge',
  './embark.automerge',
  './tiny-essay.automerge',
*/
  './larger-scene.amg',
  ]

//const Automerge = require('../../javascript/next')
//const Automerge = require('../automerge-wasm'); function mat(doc) { return doc.materialize("/",undefined,{foo:"bar"}) };
//const Automerge = require('../automerge-wasm'); function mat(doc) { return doc.materialize() };
const Automerge = require('../automerge-wasm/nodejs/automerge_wasm.cjs'); function mat(doc) { return doc.materialize() };
//const Automerge = require('../../javascript').next; function mat(doc) { return Automerge.toJS(doc) };

for (let file of files) {
  let data = fs.readFileSync(file);

  let l_time = new Date()
  let doc = Automerge.load(data)
  l_time = new Date() - l_time

  let m_time = new Date()
  mat(doc)
  m_time = new Date() - m_time

  //let p_time = new Date()
  //doc.diff([], doc.getHeads())
  //p_time = new Date() - p_time

  console.log(`Load : ${p(l_time)} ms   Materialize : ${p(m_time)} ms    -- ${file}`);
}

function p(a) {
  return ("     " + a).slice(-5)
}

