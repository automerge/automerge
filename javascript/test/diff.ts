import * as assert from "assert"
import * as A from "../src/next"

describe("the diff function", () => {
  it("should produce good error messages if heads are undefined", () => {
    const doc = A.init()
    const heads = A.getHeads(doc)
    const badHeads: A.Heads = undefined as unknown as A.Heads
    assert.throws(
      () => A.diff(doc, heads, badHeads),
      /after must be an array of strings/,
    )
    assert.throws(
      () => A.diff(doc, badHeads, heads),
      /before must be an array of strings/,
    )
  })
})
