import { Value } from "@automerge/automerge-wasm"
import { TEXT } from "./constants"

export class Text {
  elems: Value[]

  constructor (text?: string | string[]) {
    //const instance = Object.create(Text.prototype)
    if (typeof text === 'string') {
      this.elems = [...text]
    } else if (Array.isArray(text)) {
      this.elems = text
    } else if (text === undefined) {
      this.elems = []
    } else {
      throw new TypeError(`Unsupported initial value for Text: ${text}`)
    }
    Reflect.defineProperty(this, TEXT, { value: true })
  }

  get length () : number {
    return this.elems.length
  }

  get (index: number) : Value | undefined {
    return this.elems[index]
  }

  /**
   * Iterates over the text elements character by character, including any
   * inline objects.
   */
  [Symbol.iterator] () {
    const elems = this.elems
    let index = -1
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
  toString() : string {
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
  toSpans() : Value[] {
    const spans : Value[] = []
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
  toJSON() : string {
    return this.toString()
  }

  /**
   * Updates the list item at position `index` to a new value `value`.
   */
  set (index: number, value: Value) {
    this.elems[index] = value
  }

  /**
   * Inserts new list items `values` starting at position `index`.
   */
  insertAt(index: number, ...values: Value[]) {
    this.elems.splice(index, 0, ... values)
  }

  /**
   * Deletes `numDelete` list items starting at position `index`.
   * if `numDelete` is not given, one item is deleted.
   */
  deleteAt(index: number, numDelete = 1) {
    this.elems.splice(index, numDelete)
  }

  map<T>(callback: (e: Value) => T) {
    this.elems.map(callback)
  }


}

// Read-only methods that can delegate to the JavaScript built-in array
for (const method of ['concat', 'every', 'filter', 'find', 'findIndex', 'forEach', 'includes',
                    'indexOf', 'join', 'lastIndexOf', 'reduce', 'reduceRight',
                    'slice', 'some', 'toLocaleString']) {
  Text.prototype[method] = function (...args) {
    const array = [...this]
    return array[method](...args)
  }
}

