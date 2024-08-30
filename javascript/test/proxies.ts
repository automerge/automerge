import * as assert from "assert"
import { beforeEach } from "mocha"
import { type Doc, from, change } from "../src/index.js"

type DocType = {
  list: string[]
}

describe("Proxies", () => {
  let doc: Doc<DocType>
  beforeEach(() => {
    doc = from({ list: ["a", "b", "c"] })
  })

  describe("recursive document", () => {
    it("should throw a useful RangeError when attempting to set a document inside itself", () => {
      type RecursiveDoc = { [key: string]: RecursiveDoc }
      const doc = from<RecursiveDoc>({})
      change(doc, d => {
        assert.throws(() => {
          d.doc = doc
        }, /Cannot create a reference to an existing document object/)
      })
    })
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

    it("should throw a useful RangeError when attempting to splice undefined values", () => {
      const doc = from<{ list: (undefined | number)[] }>({ list: [] })
      change(doc, d => {
        assert.throws(() => {
          d.list.splice(0, 0, 5, undefined)
        }, /Cannot assign undefined value at \/list.*at index 1 in the input/)
      })
    })
  })

  describe("map proxy", () => {
    it("does allow null values", () => {
      let doc = from<any>({})
      doc = change(doc, doc => {
        doc.foo = null
      })
      assert.equal(doc.foo, null)
    })

    it("does not allow undefined values", () => {
      let doc = from<any>({})
      assert.throws(() => {
        doc = change(doc, doc => {
          doc.foo = undefined
        })
      }, "Cannot assign undefined")
    })

    it("should print the property path in the error when setting an undefined key", () => {
      const doc = from({ map: {} })
      change(doc, d => {
        assert.throws(() => {
          d.map["a"] = undefined
        }, /map\/a/)
      })
    })
  })

  describe("list proxy", () => {
    it("should print the property path in the error when setting an undefined key", () => {
      const doc = from<{ list: undefined[] }>({ list: [] })
      change(doc, d => {
        assert.throws(() => {
          d.list[0] = undefined
        }, /list\/0/)
      })
    })
  })

  describe("structuredClone support", () => {
    it("should support objects cloned with structuredClone", () => {
      const doc = from({ map: structuredClone({ key: "value", number: 2 }) })

      assert.deepEqual(doc, { map: { key: "value", number: 2 } })
    })
  })
})
