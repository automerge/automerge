import * as assert from "assert"
import { unstable as Automerge } from "../src"
import { deleteAt, insertAt } from "../src/unstable"

describe("Unstable change tests", () => {
  it("should be able to make simple changes to a document", () => {
    let doc = Automerge.from<{ foo: string | undefined }>({
      foo: "bar",
    })
    doc = Automerge.change(doc, doc => {
      assert.strictEqual(doc.foo, "bar")
      doc.foo = "baz"
    })

    assert.strictEqual(doc.foo, "baz")
  })

  it("should be able to insert into a list", () => {
    let doc = Automerge.from<{ list: string[] }>({ list: [] })
    doc = Automerge.change(doc, doc => {
      insertAt(doc.list, 0, "a")
    })
    assert.deepEqual(doc.list, ["a"])
  })

  it("should be able to delete from a list", () => {
    let doc = Automerge.from<{ list: string[] }>({ list: ["a", "b", "c"] })
    doc = Automerge.change(doc, doc => {
      deleteAt(doc.list, 0)
    })
    assert.deepEqual(doc.list, ["b", "c"])
  })
})
