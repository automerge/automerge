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

  it("should show conflicts on text objects", () => {
    let doc1 = stable.from({ text: new stable.Text("abc") }, "bb")
    let doc2 = stable.from({ text: new stable.Text("def") }, "aa")
    doc1 = stable.merge(doc1, doc2)
    let conflicts = stable.getConflicts(doc1, "text")!
    assert.equal(conflicts["1@bb"]!.toString(), "abc")
    assert.equal(conflicts["1@aa"]!.toString(), "def")

    let unstableDoc = unstable.init<any>()
    unstableDoc = unstable.merge(unstableDoc, doc1)
    let conflicts2 = unstable.getConflicts(unstableDoc, "text")!
    assert.equal(conflicts2["1@bb"]!.toString(), "abc")
    assert.equal(conflicts2["1@aa"]!.toString(), "def")
  })

  it("should allow filling a list with text in stable", () => {
    let doc = stable.from<{ list: Array<stable.Text | null> }>({
      list: [null, null, null],
    })
    doc = stable.change(doc, doc => {
      doc.list.fill(new stable.Text("abc"), 0, 3)
    })
    assert.deepStrictEqual(doc.list, [
      new stable.Text("abc"),
      new stable.Text("abc"),
      new stable.Text("abc"),
    ])
  })

  it("should allow filling a list with text in unstable", () => {
    let doc = unstable.from<{ list: Array<string | null> }>({
      list: [null, null, null],
    })
    doc = stable.change(doc, doc => {
      doc.list.fill("abc", 0, 3)
    })
    assert.deepStrictEqual(doc.list, ["abc", "abc", "abc"])
  })

  it("should allow splicing text into a list on stable", () => {
    let doc = stable.from<{ list: Array<stable.Text> }>({ list: [] })
    doc = stable.change(doc, doc => {
      doc.list.splice(0, 0, new stable.Text("abc"), new stable.Text("def"))
    })
    assert.deepStrictEqual(doc.list, [
      new stable.Text("abc"),
      new stable.Text("def"),
    ])
  })

  it("should allow splicing text into a list on unstable", () => {
    let doc = unstable.from<{ list: Array<string> }>({ list: [] })
    doc = unstable.change(doc, doc => {
      doc.list.splice(0, 0, "abc", "def")
    })
    assert.deepStrictEqual(doc.list, ["abc", "def"])
  })
})
