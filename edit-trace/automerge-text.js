// Apply the paper editing trace to an Automerge.Text object, one char at a time
const { edits, finalText } = require('./editing-trace')
const Automerge = require('../automerge-js')

const start = new Date()
let state = Automerge.from({text: new Automerge.Text()})

state = Automerge.change(state, doc => {
  for (let i = 0; i < edits.length; i++) {
    if (i % 1000 === 0) {
      console.log(`Processed ${i} edits in ${new Date() - start} ms`)
    }
    if (edits[i][1] > 0) doc.text.deleteAt(edits[i][0], edits[i][1])
    if (edits[i].length > 2) doc.text.insertAt(edits[i][0], ...edits[i].slice(2))
  }
})

if (state.text.join('') !== finalText) {
  throw new RangeError('ERROR: final text did not match expectation')
}
