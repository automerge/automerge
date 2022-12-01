import * as assert from 'assert'
import * as Automerge from '../src'
import { assertEqualsOneOf } from './helpers'

function attributeStateToAttributes(accumulatedAttributes) {
  const attributes = {}
  Object.entries(accumulatedAttributes).forEach(([key, values]) => {
    if (values.length && values[0] !== null) {
      attributes[key] = values[0]
    }
  })
  return attributes
}

function isEquivalent(a, b) {
  const aProps = Object.getOwnPropertyNames(a)
  const bProps = Object.getOwnPropertyNames(b)

  if (aProps.length != bProps.length) {
      return false
  }

  for (let i = 0; i < aProps.length; i++) {
    const propName = aProps[i]
      if (a[propName] !== b[propName]) {
          return false
      }
  }

  return true
}

function isControlMarker(pseudoCharacter) {
  return typeof pseudoCharacter === 'object' && pseudoCharacter.attributes
}

function opFrom(text, attributes) {
  let op = { insert: text }
  if (Object.keys(attributes).length > 0) {
      op.attributes = attributes
  }
  return op
}

function accumulateAttributes(span, accumulatedAttributes) {
  Object.entries(span).forEach(([key, value]) => {
    if (!accumulatedAttributes[key]) {
      accumulatedAttributes[key] = []
    }
    if (value === null) {
      if (accumulatedAttributes[key].length === 0 || accumulatedAttributes[key] === null) {
        accumulatedAttributes[key].unshift(null)
      } else {
        accumulatedAttributes[key].shift()
      }
    } else {
      if (accumulatedAttributes[key][0] === null) {
        accumulatedAttributes[key].shift()
      } else {
        accumulatedAttributes[key].unshift(value)
      }
    }
  })
  return accumulatedAttributes
}

function automergeTextToDeltaDoc(text) {
  let ops = []
  let controlState = {}
  let currentString = ""
  let attributes = {}
  text.toSpans().forEach((span) => {
    if (isControlMarker(span)) {
      controlState = accumulateAttributes(span.attributes, controlState)
    } else {
      let next = attributeStateToAttributes(controlState)

      // if the next span has the same calculated attributes as the current span
      // don't bother outputting it as a separate span, just let it ride
      if (typeof span === 'string' && isEquivalent(next, attributes)) {
          currentString = currentString + span
          return
      }

      if (currentString) {
        ops.push(opFrom(currentString, attributes))
      }

      // If we've got a string, we might be able to concatenate it to another
      // same-attributed-string, so remember it and go to the next iteration.
      if (typeof span === 'string') {
        currentString = span
        attributes = next
      } else {
        // otherwise we have an embed "character" and should output it immediately.
        // embeds are always one-"character" in length.
        ops.push(opFrom(span, next))
        currentString = ''
        attributes = {}
      }
    }
  })

  // at the end, flush any accumulated string out
  if (currentString) {
    ops.push(opFrom(currentString, attributes))
  }

  return ops
}

function inverseAttributes(attributes) {
  let invertedAttributes = {}
  Object.keys(attributes).forEach((key) => {
    invertedAttributes[key] = null
  })
  return invertedAttributes
}

function applyDeleteOp(text, offset, op) {
  let length = op.delete
  while (length > 0) {
    if (isControlMarker(text.get(offset))) {
      offset += 1
    } else {
      // we need to not delete control characters, but we do delete embed characters
      text.deleteAt(offset, 1)
      length -= 1
    }
  }
  return [text, offset]
}

function applyRetainOp(text, offset, op) {
  let length = op.retain

  if (op.attributes) {
    text.insertAt(offset, { attributes: op.attributes })
    offset += 1
  }

  while (length > 0) {
    const char = text.get(offset)
    offset += 1
    if (!isControlMarker(char)) {
      length -= 1
    }
  }

  if (op.attributes) {
    text.insertAt(offset, { attributes: inverseAttributes(op.attributes) })
    offset += 1
  }

  return [text, offset]
}


function applyInsertOp(text, offset, op) {
  let originalOffset = offset

  if (typeof op.insert === 'string') {
    text.insertAt(offset, ...op.insert.split(''))
    offset += op.insert.length
  } else {
    // we have an embed or something similar
    text.insertAt(offset, op.insert)
    offset += 1
  }

  if (op.attributes) {
    text.insertAt(originalOffset, { attributes: op.attributes })
    offset += 1
  }
  if (op.attributes) {
    text.insertAt(offset, { attributes: inverseAttributes(op.attributes) })
    offset += 1
  }
  return [text, offset]
}

// XXX: uhhhhh, why can't I pass in text?
function applyDeltaDocToAutomergeText(delta, doc) {
  let offset = 0

  delta.forEach(op => {
    if (op.retain) {
      [, offset] = applyRetainOp(doc.text, offset, op)
    } else if (op.delete) {
      [, offset] = applyDeleteOp(doc.text, offset, op)
    } else if (op.insert) {
      [, offset] = applyInsertOp(doc.text, offset, op)
    }
  })
}

describe('Automerge.Text', () => {
  let s1, s2
  beforeEach(() => {
    s1 = Automerge.change(Automerge.init(), doc => doc.text = "")
    s2 = Automerge.merge(Automerge.init(), s1)
  })

  it('should support insertion', () => {
    s1 = Automerge.change(s1, doc => Automerge.splice(doc, "text", 0, 0, "a"))
    assert.strictEqual(s1.text.length, 1)
    assert.strictEqual(s1.text[0], 'a')
    assert.strictEqual(s1.text, 'a')
    //assert.strictEqual(s1.text.getElemId(0), `2@${Automerge.getActorId(s1)}`)
  })

  it('should support deletion', () => {
    s1 = Automerge.change(s1, doc => Automerge.splice(doc, "text", 0, 0, "abc"))
    s1 = Automerge.change(s1, doc => Automerge.splice(doc, "text", 1, 1))
    assert.strictEqual(s1.text.length, 2)
    assert.strictEqual(s1.text[0], 'a')
    assert.strictEqual(s1.text[1], 'c')
    assert.strictEqual(s1.text, 'ac')
  })

  it("should support implicit and explicit deletion", () => {
    s1 = Automerge.change(s1, doc => Automerge.splice(doc, "text", 0, 0, "abc"))
    s1 = Automerge.change(s1, doc => Automerge.splice(doc, "text", 1, 1))
    s1 = Automerge.change(s1, doc => Automerge.splice(doc, "text", 1, 0))
    assert.strictEqual(s1.text.length, 2)
    assert.strictEqual(s1.text[0], "a")
    assert.strictEqual(s1.text[1], "c")
    assert.strictEqual(s1.text, "ac")
  })

  it('should handle concurrent insertion', () => {
    s1 = Automerge.change(s1, doc => Automerge.splice(doc, "text", 0, 0, "abc"))
    s2 = Automerge.change(s2, doc => Automerge.splice(doc, "text", 0, 0, "xyz"))
    s1 = Automerge.merge(s1, s2)
    assert.strictEqual(s1.text.length, 6)
    assertEqualsOneOf(s1.text, 'abcxyz', 'xyzabc')
  })

  it('should handle text and other ops in the same change', () => {
    s1 = Automerge.change(s1, doc => {
      doc.foo = 'bar'
      Automerge.splice(doc, "text", 0, 0, 'a')
    })
    assert.strictEqual(s1.foo, 'bar')
    assert.strictEqual(s1.text, 'a')
    assert.strictEqual(s1.text, 'a')
  })

  it('should serialize to JSON as a simple string', () => {
    s1 = Automerge.change(s1, doc => Automerge.splice(doc, "text", 0, 0, 'a"b'))
    assert.strictEqual(JSON.stringify(s1), '{"text":"a\\"b"}')
  })

  it('should allow modification after an object is assigned to a document', () => {
    s1 = Automerge.change(Automerge.init(), doc => {
      doc.text = ""
      Automerge.splice(doc ,"text", 0, 0, 'abcd')
      Automerge.splice(doc ,"text", 2, 1)
      assert.strictEqual(doc.text, 'abd')
    })
    assert.strictEqual(s1.text, 'abd')
  })

  it('should not allow modification outside of a change callback', () => {
    assert.throws(() => Automerge.splice(s1 ,"text", 0, 0, 'a'), /object cannot be modified outside of a change block/)
  })

  describe('with initial value', () => {

    it('should initialize text in Automerge.from()', () => {
      let s1 = Automerge.from({text: 'init'})
      assert.strictEqual(s1.text.length, 4)
      assert.strictEqual(s1.text[0], 'i')
      assert.strictEqual(s1.text[1], 'n')
      assert.strictEqual(s1.text[2], 'i')
      assert.strictEqual(s1.text[3], 't')
      assert.strictEqual(s1.text, 'init')
    })

    it('should encode the initial value as a change', () => {
      const s1 = Automerge.from({text: 'init'})
      const changes = Automerge.getAllChanges(s1)
      assert.strictEqual(changes.length, 1)
      const [s2] = Automerge.applyChanges(Automerge.init(), changes)
      assert.strictEqual(s2.text, 'init')
      assert.strictEqual(s2.text, 'init')
    })

  })

  it('should support unicode when creating text', () => {
    s1 = Automerge.from({
      text: '🐦'
    })
    assert.strictEqual(s1.text, '🐦')
  })
})
