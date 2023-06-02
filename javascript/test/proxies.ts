import * as assert from "assert"
import { beforeEach } from "mocha"
import { type Doc, from, change } from "../src"

type DocType = {
  list: string[]
}

describe("Proxy Tests", () => {
  describe("Iterators", () => {
    let doc: Doc<DocType>
    beforeEach(() => {
      doc = from({ list: ["a", "b", "c"] })
    })

    it("Lists should return iterable entries", () => {
      change(doc, d => {
        let count = 0

        for (const [index, value] of d.list.entries()) {
          assert.equal(value, d.list[index])
          count++
        }

        assert.equal(count, 3)
      })
    })

    it("Lists should return iterable values", () => {
      change(doc, d => {
        let count = 0

        for (const value of d.list.values()) {
          assert.equal(value, d.list[count++])
        }

        assert.equal(count, 3)
      })
    })

    it("Lists should return iterable keys", () => {
      change(doc, d => {
        let count = 3

        for (const index of d.list.keys()) {
          assert.equal(index + count--, 3)
        }

        assert.equal(count, 0)
      })
    })
  })
})
