const fs = require('fs');
const util = require('util');
//const data = fs.readFileSync('./slow.automerge');
//const data = fs.readFileSync('./slow-font.automerge');
const data = fs.readFileSync('./embark.automerge');
//const data = fs.readFileSync('./tiny-essay.automerge');
//const data = fs.readFileSync('./larger-scene.amg');
//const { edits, finalText } = require('./editing-trace')
const Automerge = require('../../javascript').next

const start = new Date()

let t_time = new Date()
//let doc1 = Automerge.load(data);
//console.log(`Load in ${new Date() - t_time} ms`)
t_time = new Date()

let doc2 = Automerge.init();
doc2 = Automerge.loadIncremental(doc2, data);
console.log(`Load Inc in ${new Date() - t_time} ms`)

t_time = new Date()
let doc3 = Automerge.clone(doc2);
console.log(`Fork in ${new Date() - t_time} ms`)

t_time = new Date()
let doc3a = Automerge.clone(doc2,Automerge.getHeads(doc2));
console.log(`ForkAt in ${new Date() - t_time} ms`)

t_time = new Date()
let doc4 = Automerge.load(Automerge.save(doc3));
console.log(`Save/load in ${new Date() - t_time} ms`)

/*
doc.enablePatches(true)
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

let t_time = new Date()
let saved = doc.save()
console.log(`doc.save in ${new Date() - t_time} ms`)

t_time = new Date()
Automerge.load(saved)
console.log(`doc.load in ${new Date() - t_time} ms`)

t_time = new Date()
let t = doc.text(text);
console.log(`doc.text in ${new Date() - t_time} ms`)

t_time = new Date()
t = doc.text(text);
mat = doc.applyPatches(mat)
console.log(`doc.applyPatches() in ${new Date() - t_time} ms`)

if (doc.text(text) !== finalText) {
  throw new RangeError('ERROR: final text did not match expectation')
}
*/
