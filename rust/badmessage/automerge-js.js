// Apply the paper editing trace to an Automerge.Text object, one char at a time
const Automerge = require('../../javascript')

const fs = require('fs');

const start = new Date()

let contents = fs.readFileSync("badmessage");
let doc = Automerge.init();
let state = Automerge.initSyncState();
[doc,state] = Automerge.receiveSyncMessage(doc, state, contents);

console.log(`doc.receiveSyncMessage in ${new Date() - start} ms`)

let t_time = new Date()
let saved = Automerge.save(doc);
console.log(`doc.save in               ${new Date() - t_time} ms`)

t_time = new Date()
Automerge.load(saved)
console.log(`doc.load in               ${new Date() - t_time} ms`)

t_time = new Date()
let doc2 = Automerge.init()
doc2 = Automerge.loadIncremental(doc2,saved)
console.log(`doc.loadIncremental in    ${new Date() - t_time} ms`)
