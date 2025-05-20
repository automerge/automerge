import * as assert from "assert"
import * as Automerge from "../src/index.js"

describe("Automerge", () => {
  describe("basics", () => {
    it("should allow you to load incrementally", () => {
      let doc1 = Automerge.from<any>({ foo: "bar" })
      let doc2 = Automerge.init<any>()
      doc2 = Automerge.loadIncremental(doc2, Automerge.save(doc1))
      doc1 = Automerge.change(doc1, d => (d.foo2 = "bar2"))
      doc2 = Automerge.loadIncremental(
        doc2,
        Automerge.getBackend(doc1).saveIncremental(),
      )
      doc1 = Automerge.change(doc1, d => (d.foo = "bar2"))
      doc2 = Automerge.loadIncremental(
        doc2,
        Automerge.getBackend(doc1).saveIncremental(),
      )
      doc1 = Automerge.change(doc1, d => (d.x = "y"))
      doc2 = Automerge.loadIncremental(
        doc2,
        Automerge.getBackend(doc1).saveIncremental(),
      )
      assert.deepEqual(doc1, doc2)
    })
  })
})
