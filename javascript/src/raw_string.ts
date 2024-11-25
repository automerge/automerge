import { RAW_STRING } from "./constants.js"

export class RawString {
  // Used to detect whether a value is a RawString object rather than using an instanceof check
  [RAW_STRING] = true
  val: string
  constructor(val: string) {
    this.val = val
  }

  /**
   * Returns the content of the RawString object as a simple string
   */
  toString(): string {
    return this.val
  }

  toJSON(): string {
    return this.val
  }
}
