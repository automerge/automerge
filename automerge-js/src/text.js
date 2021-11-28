const { OBJECT_ID } = require('./constants')
const { isObject } = require('../src/common')

class Text {
  constructor (text) {
    if (typeof text === 'string') {
      const elems = [...text].map(value => ({value}))
      return instantiateText(undefined, elems) // eslint-disable-line
    } else if (Array.isArray(text)) {
      const elems = text.map(value => ({value}))
      return instantiateText(undefined, elems) // eslint-disable-line
    } else if (text === undefined) {
      return instantiateText(undefined, []) // eslint-disable-line
    } else {
      throw new TypeError(`Unsupported initial value for Text: ${text}`)
    }
  }

  get length () {
    return this.elems.length
  }

  get (index) {
    const value = this.elems[index].value
    return value
  }

  getElemId (index) {
    return this.elems[index].elemId
  }

  /**
   * Iterates over the text elements character by character, including any
   * inline objects.
   */
  [Symbol.iterator] () {
    let elems = this.elems, index = -1
    return {
      next () {
        index += 1
        if (index < elems.length) {
          return {done: false, value: elems[index].value}
        } else {
          return {done: true}
        }
      }
    }
  }

  /**
   * Returns the content of the Text object as a simple string, ignoring any
   * non-character elements.
   */
  toString() {
    // Concatting to a string is faster than creating an array and then
    // .join()ing for small (<100KB) arrays.
    // https://jsperf.com/join-vs-loop-w-type-test
    let str = ''
    for (const elem of this.elems) {
      if (typeof elem.value === 'string') str += elem.value
    }
    return str
  }

  /**
   * Returns the content of the Text object as a sequence of strings,
   * interleaved with non-character elements.
   *
   * For example, the value ['a', 'b', {x: 3}, 'c', 'd'] has spans:
   * => ['ab', {x: 3}, 'cd']
   */
  toSpans() {
    let spans = []
    let chars = ''
    for (const elem of this.elems) {
      if (typeof elem.value === 'string') {
        chars += elem.value
      } else {
        if (chars.length > 0) {
          spans.push(chars)
          chars = ''
        }
        spans.push(elem.value)
      }
    }
    if (chars.length > 0) {
      spans.push(chars)
    }
    return spans
  }

  /**
   * Returns the content of the Text object as a simple string, so that the
   * JSON serialization of an Automerge document represents text nicely.
   */
  toJSON() {
    return this.toString()
  }

  /**
   * Updates the list item at position `index` to a new value `value`.
   */
  set (index, value) {
    throw new TypeError('Automerge.Text object cannot be modified outside of a change block')
  }

  /**
   * Inserts new list items `values` starting at position `index`.
   */
  insertAt(index, ...values) {
    throw new TypeError('Automerge.Text object cannot be modified outside of a change block')
  }

  /**
   * Deletes `numDelete` list items starting at position `index`.
   * if `numDelete` is not given, one item is deleted.
   */
  deleteAt(index, numDelete = 1) {
    throw new TypeError('Automerge.Text object cannot be modified outside of a change block')
  }
}

// Read-only methods that can delegate to the JavaScript built-in array
for (let method of ['concat', 'every', 'filter', 'find', 'findIndex', 'forEach', 'includes',
                    'indexOf', 'join', 'lastIndexOf', 'map', 'reduce', 'reduceRight',
                    'slice', 'some', 'toLocaleString']) {
  Text.prototype[method] = function (...args) {
    const array = [...this]
    return array[method](...args)
  }
}

function instantiateText(objectId, elems) {
  const instance = Object.create(Text.prototype)
  instance[OBJECT_ID] = objectId
  instance.elems = elems
  return instance
}

module.exports = { Text, instantiateText }
