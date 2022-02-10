
// make sure to 

// # cd ../automerge-wasm
// # yarn release
// # yarn opt

const { edits, finalText } = require('./editing-trace')
const Automerge = require('../automerge-wasm')

const start = new Date()

let doc = Automerge.init();
let text = doc.set("_root", "text", Automerge.TEXT)

for (let i = 0; i < edits.length; i++) {
  let edit = edits[i]
  if (i % 1000 === 0) {
    console.log(`Processed ${i} edits in ${new Date() - start} ms`)
  }
  doc.splice(text, ...edit)
}

let _ = doc.save()

console.log(`Done in ${new Date() - start} ms`)

let t_time = new Date()
let t = doc.text(text);
console.log(`doc.text in ${new Date() - t_time} ms`)

if (doc.text(text) !== finalText) {
  throw new RangeError('ERROR: final text did not match expectation')
}
