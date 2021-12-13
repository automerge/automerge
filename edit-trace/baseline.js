// Apply the paper editing trace to a regular JavaScript array (using .splice, one char at a time)
const { edits, finalText } = require('./editing-trace')

const start = new Date()
let chars = []
for (let i = 0; i < edits.length; i++) {
  let edit = edits[i]
  if (i % 1000 === 0) {
    console.log(`Processed ${i} edits in ${new Date() - start} ms`)
  }
  chars.splice(...edit)
}

let _save = JSON.stringify(chars)

const time = new Date() - start

console.log(`Done in ${time} ms`)

if (chars.join('') !== finalText) {
  throw new RangeError('ERROR: final text did not match expectation')
}

