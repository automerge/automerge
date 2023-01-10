import * as assert from "assert"
import * as stable from "../src"
import { unstable } from "../src"

describe("stable/unstable interop", () => {
  it("should allow reading Text from stable as strings in unstable", () => {
    let stableDoc = stable.from({
      text: new stable.Text("abc"),
    })
    let unstableDoc = unstable.init<any>()
    unstableDoc = unstable.merge(unstableDoc, stableDoc)
    assert.deepStrictEqual(unstableDoc.text, "abc")
  })

  it("should allow string from stable as Text in unstable", () => {
    let unstableDoc = unstable.from({
      text: "abc",
    })
    let stableDoc = stable.init<any>()
    stableDoc = unstable.merge(stableDoc, unstableDoc)
    assert.deepStrictEqual(stableDoc.text, new stable.Text("abc"))
  })

  it("should allow reading strings from stable as RawString in unstable", () => {
    let stableDoc = stable.from({
      text: "abc",
    })
    let unstableDoc = unstable.init<any>()
    unstableDoc = unstable.merge(unstableDoc, stableDoc)
    assert.deepStrictEqual(unstableDoc.text, new unstable.RawString("abc"))
  })

  it("should allow reading RawString from unstable as string in stable", () => {
    let unstableDoc = unstable.from({
      text: new unstable.RawString("abc"),
    })
    let stableDoc = stable.init<any>()
    stableDoc = unstable.merge(stableDoc, unstableDoc)
    assert.deepStrictEqual(stableDoc.text, "abc")
  })
})
