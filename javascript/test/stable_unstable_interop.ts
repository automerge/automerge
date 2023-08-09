import * as assert from "assert"
import * as old from "../src"
import { next } from "../src"

describe("old/next interop", () => {
  it("should allow reading Text from old as strings in next", () => {
    let nextDoc = old.from({
      text: new old.Text("abc"),
    })
    let oldDoc = next.init<any>()
    oldDoc = next.merge(oldDoc, nextDoc)
    assert.deepStrictEqual(oldDoc.text, "abc")
  })

  it("should allow string from old as Text in next", () => {
    let nextDoc = next.from({
      text: "abc",
    })
    let oldDoc = old.init<any>()
    oldDoc = next.merge(oldDoc, nextDoc)
    assert.deepStrictEqual(oldDoc.text, new old.Text("abc"))
  })

  it("should allow reading strings from old as RawString in next", () => {
    let nextDoc = old.from({
      text: "abc",
    })
    let oldDoc = next.init<any>()
    oldDoc = next.merge(oldDoc, nextDoc)
    assert.deepStrictEqual(oldDoc.text, new next.RawString("abc"))
  })

  it("should allow reading RawString from next as string in old", () => {
    let nextDoc = next.from({
      text: new next.RawString("abc"),
    })
    let oldDoc = old.init<any>()
    oldDoc = next.merge(oldDoc, nextDoc)
    assert.deepStrictEqual(oldDoc.text, "abc")
  })

  it("should show conflicts on text objects", () => {
    let doc1 = old.from({ text: new old.Text("abc") }, "bb")
    let doc2 = old.from({ text: new old.Text("def") }, "aa")
    doc1 = old.merge(doc1, doc2)
    let conflicts = old.getConflicts(doc1, "text")!
    assert.equal(conflicts["1@bb"]!.toString(), "abc")
    assert.equal(conflicts["1@aa"]!.toString(), "def")

    let nextDoc = next.init<any>()
    nextDoc = next.merge(nextDoc, doc1)
    let conflicts2 = next.getConflicts(nextDoc, "text")!
    assert.equal(conflicts2["1@bb"]!.toString(), "abc")
    assert.equal(conflicts2["1@aa"]!.toString(), "def")
  })

  it("should allow filling a list with text in old", () => {
    let doc = old.from<{ list: Array<old.Text | null> }>({
      list: [null, null, null],
    })
    doc = old.change(doc, doc => {
      doc.list.fill(new old.Text("abc"), 0, 3)
    })
    assert.deepStrictEqual(doc.list, [
      new old.Text("abc"),
      new old.Text("abc"),
      new old.Text("abc"),
    ])
  })

  it("should allow filling a list with text in next", () => {
    let doc = next.from<{ list: Array<string | null> }>({
      list: [null, null, null],
    })
    doc = old.change(doc, doc => {
      doc.list.fill("abc", 0, 3)
    })
    assert.deepStrictEqual(doc.list, ["abc", "abc", "abc"])
  })

  it("should allow splicing text into a list on old", () => {
    let doc = old.from<{ list: Array<old.Text> }>({ list: [] })
    doc = old.change(doc, doc => {
      doc.list.splice(0, 0, new old.Text("abc"), new old.Text("def"))
    })
    assert.deepStrictEqual(doc.list, [new old.Text("abc"), new old.Text("def")])
  })

  it("should allow splicing text into a list on next", () => {
    let doc = next.from<{ list: Array<string> }>({ list: [] })
    doc = next.change(doc, doc => {
      doc.list.splice(0, 0, "abc", "def")
    })
    assert.deepStrictEqual(doc.list, ["abc", "def"])
  })
})
