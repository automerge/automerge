import { IMMUTABLE_STRING } from "./constants.js"

export class ImmutableString {
  // Used to detect whether a value is a ImmutableString object rather than using an instanceof check
  [IMMUTABLE_STRING] = true
  val: string
  constructor(val: string) {
    this.val = val
  }

  /**
   * Returns the content of the ImmutableString object as a simple string
   */
  toString(): string {
    return this.val
  }

  toJSON(): string {
    return this.val
  }
}
