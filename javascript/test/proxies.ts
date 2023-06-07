import * as assert from "assert"
import { beforeEach } from "mocha"
import { type Doc, from, change } from "../src"

type DocType = {
  list: string[]
}

describe("Proxies", () => {
  let doc: Doc<DocType>
  beforeEach(() => {
    doc = from({ list: ["a", "b", "c"] })
  })

  describe("List Iterators", () => {
    it("should return iterable entries", () => {
      change(doc, d => {
        let count = 0

        for (const [index, value] of d.list.entries()) {
          assert.equal(value, d.list[index])
          count++
        }

        assert.equal(count, 3)
      })
    })

    it("should return iterable values", () => {
      change(doc, d => {
        let count = 0

        for (const value of d.list.values()) {
          assert.equal(value, d.list[count++])
        }

        assert.equal(count, 3)
      })
    })

    it("should return iterable keys", () => {
      change(doc, d => {
        let count = 3

        for (const index of d.list.keys()) {
          assert.equal(index + count--, 3)
        }

        assert.equal(count, 0)
      })
    })
  })

  describe("List splice", () => {
    it("should be able to remove a defined number of list entries", () => {
      doc = change(doc, d => {
        const deleted = d.list.splice(1, 1)
        assert.deepEqual(deleted, ["b"])
      })

      assert.deepEqual(doc.list, ["a", "c"])
    })

    it("should be able to remove a defined number of list entries and add new ones", () => {
      doc = change(doc, d => {
        const deleted = d.list.splice(1, 1, "d", "e")
        assert.deepEqual(deleted, ["b"])
      })

      assert.deepEqual(doc.list, ["a", "d", "e", "c"])
    })

    it("should be able to insert new values", () => {
      doc = change(doc, d => {
        const deleted = d.list.splice(1, 0, "d", "e")
        assert.deepEqual(deleted, [])
      })

      assert.deepEqual(doc.list, ["a", "d", "e", "b", "c"])
    })

    it("should work with only a start parameter", () => {
      doc = change(doc, d => {
        const deleted = d.list.splice(1)
        assert.deepEqual(deleted, ["b", "c"])
      })

      assert.deepEqual(doc.list, ["a"])
    })
  })
})
