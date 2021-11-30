// Apply the paper editing trace to an Automerge.Text object, one char at a time
const { edits, finalText } = require('./editing-trace')
const Automerge = require('../automerge-wasm')

const start = new Date()

const root = Automerge.root();
let doc = Automerge.init();
doc.begin()
let text = doc.set(root, "text", Automerge.TEXT)
doc.commit()

doc.begin();

for (let i = 0; i < edits.length; i++) {
  let edit = edits[i]
  if (i % 1000 === 0) {
    console.log(`Processed ${i} edits in ${new Date() - start} ms`)
  }
  doc.splice(text, ...edit)
}
doc.commit()

//if (state.text.join('') !== finalText) {
//  throw new RangeError('ERROR: final text did not match expectation')
//}
