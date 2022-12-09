const Automerge = require('../automerge-wasm')
const fs = require('fs');


let contents = fs.readFileSync("badmessage");
let doc = Automerge.create();
let state = Automerge.initSyncState();

let t_time = new Date()

doc.receiveSyncMessage(state,contents);

console.log(`doc.receiveSyncMessage in ${new Date() - t_time} ms`)

t_time = new Date()
let saved = doc.save()
console.log(`doc.save in               ${new Date() - t_time} ms`)

t_time = new Date()
Automerge.load(saved)
console.log(`doc.load in               ${new Date() - t_time} ms`)

t_time = new Date()
let doc2 = Automerge.create()
doc2.loadIncremental(saved)
console.log(`doc.loadIncremental in    ${new Date() - t_time} ms`)
