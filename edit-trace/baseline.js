// Apply the paper editing trace to a regular JavaScript array (using .splice, one char at a time)
const { edits, finalText } = require('./editing-trace')

const start = new Date()
let chars = []
for (let edit of edits) chars.splice(...edit)
const time = new Date() - start

if (chars.join('') !== finalText) {
  throw new RangeError('ERROR: final text did not match expectation')
}

console.log(`Applied ${edits.length} edits in ${time} ms`)
