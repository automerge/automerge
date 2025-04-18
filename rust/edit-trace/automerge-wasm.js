const { edits, finalText } = require('./editing-trace')
const Automerge = require('../automerge-wasm')

const start = new Date()

let doc = Automerge.create();
let mat = doc.materialize("/")
let text = doc.putObject("_root", "text", "", "text")

for (let i = 0; i < edits.length; i++) {
  let edit = edits[i]
  if (i % 10000 === 0) {
    console.log(`Processed ${i} edits in ${new Date() - start} ms`)
  }
  doc.splice(text, ...edit)
}

console.log(`Done in ${new Date() - start} ms`)

/*
let t_time = new Date()
let saved = doc.save()
console.log(`doc.save in ${new Date() - t_time} ms`)

t_time = new Date()
Automerge.load(saved)
console.log(`doc.load in ${new Date() - t_time} ms`)

t_time = new Date()
doc.fork(undefined, doc.getHeads())
console.log(`doc.forkAt in ${new Date() - t_time} ms`)

t_time = new Date()
let t = doc.text(text);
console.log(`doc.text in ${new Date() - t_time} ms`)

t_time = new Date()
t = doc.text(text);
mat = doc.applyPatches(mat)
console.log(`doc.applyPatches() in ${new Date() - t_time} ms`)
*/

if (doc.text(text) !== finalText) {
  throw new RangeError('ERROR: final text did not match expectation')
}
