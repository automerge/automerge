import type { Value } from "@automerge/automerge-wasm"
import { TEXT, STATE } from "./constants"
import type { InternalState } from "./internal_state"

export class Text {
  //eslint-disable-next-line @typescript-eslint/no-explicit-any
  elems: Array<any>
  str: string | undefined
  //eslint-disable-next-line @typescript-eslint/no-explicit-any
  spans: Array<any> | undefined;
  //eslint-disable-next-line @typescript-eslint/no-explicit-any
  [STATE]?: InternalState<any>

  constructor(text?: string | string[] | Value[]) {
    if (typeof text === "string") {
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

  get length(): number {
    return this.elems.length
  }

  //eslint-disable-next-line @typescript-eslint/no-explicit-any
  get(index: number): any {
    return this.elems[index]
  }

  /**
   * Iterates over the text elements character by character, including any
   * inline objects.
   */
  [Symbol.iterator]() {
    const elems = this.elems
    let index = -1
    return {
      next() {
        index += 1
        if (index < elems.length) {
          return { done: false, value: elems[index] }
        } else {
          return { done: true }
        }
      },
    }
  }

  /**
   * Returns the content of the Text object as a simple string, ignoring any
   * non-character elements.
   */
  toString(): string {
    if (!this.str) {
      // Concatting to a string is faster than creating an array and then
      // .join()ing for small (<100KB) arrays.
      // https://jsperf.com/join-vs-loop-w-type-test
      this.str = ""
      for (const elem of this.elems) {
        if (typeof elem === "string") this.str += elem
        else this.str += "\uFFFC"
      }
    }
    return this.str
  }

  /**
   * Returns the content of the Text object as a sequence of strings,
   * interleaved with non-character elements.
   *
   * For example, the value `['a', 'b', {x: 3}, 'c', 'd']` has spans:
   * `=> ['ab', {x: 3}, 'cd']`
   */
  toSpans(): Array<Value | object> {
    if (!this.spans) {
      this.spans = []
      let chars = ""
      for (const elem of this.elems) {
        if (typeof elem === "string") {
          chars += elem
        } else {
          if (chars.length > 0) {
            this.spans.push(chars)
            chars = ""
          }
          this.spans.push(elem)
        }
      }
      if (chars.length > 0) {
        this.spans.push(chars)
      }
    }
    return this.spans
  }

  /**
   * Returns the content of the Text object as a simple string, so that the
   * JSON serialization of an Automerge document represents text nicely.
   */
  toJSON(): string {
    return this.toString()
  }

  /**
   * Updates the list item at position `index` to a new value `value`.
   */
  set(index: number, value: Value) {
    if (this[STATE]) {
      throw new RangeError(
        "object cannot be modified outside of a change block",
      )
    }
    this.elems[index] = value
  }

  /**
   * Inserts new list items `values` starting at position `index`.
   */
  insertAt(index: number, ...values: Array<Value | object>) {
    if (this[STATE]) {
      throw new RangeError(
        "object cannot be modified outside of a change block",
      )
    }
    if (values.every(v => typeof v === "string")) {
      this.elems.splice(index, 0, ...values.join(""))
    } else {
      this.elems.splice(index, 0, ...values)
    }
  }

  /**
   * Deletes `numDelete` list items starting at position `index`.
   * if `numDelete` is not given, one item is deleted.
   */
  deleteAt(index: number, numDelete = 1) {
    if (this[STATE]) {
      throw new RangeError(
        "object cannot be modified outside of a change block",
      )
    }
    this.elems.splice(index, numDelete)
  }

  map<T>(callback: (e: Value | object) => T) {
    this.elems.map(callback)
  }

  lastIndexOf(searchElement: Value, fromIndex?: number) {
    this.elems.lastIndexOf(searchElement, fromIndex)
  }

  concat(other: Text): Text {
    return new Text(this.elems.concat(other.elems))
  }

  every(test: (v: Value) => boolean): boolean {
    return this.elems.every(test)
  }

  filter(test: (v: Value) => boolean): Text {
    return new Text(this.elems.filter(test))
  }

  find(test: (v: Value) => boolean): Value | undefined {
    return this.elems.find(test)
  }

  findIndex(test: (v: Value) => boolean): number | undefined {
    return this.elems.findIndex(test)
  }

  forEach(f: (v: Value) => undefined) {
    this.elems.forEach(f)
  }

  includes(elem: Value): boolean {
    return this.elems.includes(elem)
  }

  indexOf(elem: Value) {
    return this.elems.indexOf(elem)
  }

  join(sep?: string): string {
    return this.elems.join(sep)
  }

  reduce(
    f: (
      previousValue: Value,
      currentValue: Value,
      currentIndex: number,
      array: Value[],
    ) => Value,
  ) {
    this.elems.reduce(f)
  }

  reduceRight(
    f: (
      previousValue: Value,
      currentValue: Value,
      currentIndex: number,
      array: Value[],
    ) => Value,
  ) {
    this.elems.reduceRight(f)
  }

  slice(start?: number, end?: number) {
    return new Text(this.elems.slice(start, end))
  }

  some(test: (arg: Value) => boolean): boolean {
    return this.elems.some(test)
  }

  toLocaleString() {
    this.toString()
  }
}
