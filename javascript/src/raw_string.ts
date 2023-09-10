export class RawString {
  val: string
  constructor(val: string) {
    this.val = val
  }

  /**
   * Returns the content of the RawString object as a simple string
   */
  toString(): string {
    return this.val;
  }
}
