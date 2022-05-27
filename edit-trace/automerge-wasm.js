
// make sure to

// # cd ../automerge-wasm
// # yarn release

const { edits, finalText } = require('./editing-trace')
const Automerge = require('../automerge-wasm')

const start = new Date()

let doc = Automerge.create();
doc.enablePatches(true)
let text = doc.putObject("_root", "text", "", "text")

for (let i = 0; i < edits.length; i++) {
  let edit = edits[i]
  if (i % 10000 === 0) {
    console.log(`Processed ${i} edits in ${new Date() - start} ms`)
  }
  doc.splice(text, ...edit)
}

let _ = doc.save()

console.log(`Done in ${new Date() - start} ms`)

let t_time = new Date()
let t = doc.text(text);
console.log(`doc.text in ${new Date() - t_time} ms`)

let p_time = new Date()
let p = doc.popPatches();
console.log(`doc.popPatches in ${new Date() - p_time} ms`)

if (doc.text(text) !== finalText) {
  throw new RangeError('ERROR: final text did not match expectation')
}
