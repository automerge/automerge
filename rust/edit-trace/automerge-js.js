// Apply the paper editing trace to an Automerge.Text object, one char at a time
const { edits, finalText } = require('./editing-trace')
const Automerge = require('../../javascript')

const start = new Date()
let state = Automerge.from({text: ""})

state = Automerge.change(state, doc => {
  for (let i = 0; i < edits.length; i++) {
    if (i % 10000 === 0) {
      console.log(`Processed ${i} edits in ${new Date() - start} ms`)
    }
    let edit = edits[i]
    Automerge.splice(doc, 'text', ... edit)
  }
})

let _ = Automerge.save(state)
console.log(`Done in ${new Date() - start} ms`)

if (state.text !== finalText) {
  throw new RangeError('ERROR: final text did not match expectation')
}
