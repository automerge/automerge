import { default as assert } from "assert"
import { next as Automerge } from "../src/entrypoints/fullfat_node.js"

describe("The frontier", () => {
  describe("basics", () => {
    it("should init clone and free", () => {
      let doc1 = Automerge.from({ foo: "bar" })
      const startHeads = Automerge.getHeads(doc1)

      let left = Automerge.clone(doc1)
      left = Automerge.change(left, d => {
        d.foo = "baz"
      })
      const leftHeads = Automerge.getHeads(left)

      let right = Automerge.clone(doc1)
      right = Automerge.change(right, d => {
        d.foo = "quz"
      })
      const rightHeads = Automerge.getHeads(right)

      doc1 = Automerge.merge(doc1, left)
      doc1 = Automerge.merge(doc1, right)

      const frontier = Automerge.frontier(doc1, [
        ...startHeads,
        ...leftHeads,
        ...rightHeads,
      ])
      const expected = new Set([...rightHeads, ...leftHeads])
      const actual = new Set(frontier)
      assert.deepEqual(expected, actual)
    })
  })
})
