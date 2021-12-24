const { OBJECT_ID } = require('./constants')
const { isObject } = require('../src/common')

class Text {
  constructor (text) {
    const instance = Object.create(Text.prototype)
    if (typeof text === 'string') {
      instance.elems = [...text]
    } else if (Array.isArray(text)) {
      instance.elems = text
    } else if (text === undefined) {
      instance.elems = []
    } else {
      throw new TypeError(`Unsupported initial value for Text: ${text}`)
    }
    return instance
  }

  get length () {
    return this.elems.length
  }

  get (index) {
    return this.elems[index]
  }

  getElemId (index) {
    return undefined
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
          return {done: false, value: elems[index]}
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
      if (typeof elem === 'string') str += elem
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
      if (typeof elem === 'string') {
        chars += elem
      } else {
        if (chars.length > 0) {
          spans.push(chars)
          chars = ''
        }
        spans.push(elem)
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
    this.elems[index] = value
  }

  /**
   * Inserts new list items `values` starting at position `index`.
   */
  insertAt(index, ...values) {
    this.elems.splice(index, 0, ... values)
  }

  /**
   * Deletes `numDelete` list items starting at position `index`.
   * if `numDelete` is not given, one item is deleted.
   */
  deleteAt(index, numDelete = 1) {
    this.elems.splice(index, numDelete)
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

module.exports = { Text }
