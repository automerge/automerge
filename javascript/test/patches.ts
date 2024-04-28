import * as assert from "assert"
import { next as Automerge } from "../src/index.js"
import { type List } from "../src/index.js"

describe("patches", () => {
  describe("the patchCallback", () => {
    it("should provide access to before and after states", () => {
      const doc = Automerge.init<{ count: number }>()
      const headsBefore = Automerge.getHeads(doc)
      let headsAfter

      const newDoc = Automerge.change(
        doc,
        {
          patchCallback: (_, patchInfo) => {
            assert.deepEqual(Automerge.getHeads(patchInfo.before), headsBefore)
            headsAfter = Automerge.getHeads(patchInfo.after) // => error: recursive use of an object detected which would lead to unsafe aliasing in rust
          },
        },
        doc => {
          doc.count = 1
        },
      )
      assert.deepEqual(headsAfter, Automerge.getHeads(newDoc))
    })

    it("should provide correct before and after states when an array has a value deleted", () => {
      const doc = Automerge.from<{ list: string[] }>({ list: ["a", "b", "c"] })

      const newDoc = Automerge.change(
        doc,
        {
          patchCallback: (_, patchInfo) => {
            assert.deepEqual(
              patchInfo.before.list,
              ["a", "b", "c"],
              "before should be the original list",
            )
            assert.deepEqual(patchInfo.after.list, ["a", "c"])
          },
        },
        doc => {
          Automerge.deleteAt(doc.list, 1)
        },
      )
      assert.deepEqual(newDoc, { list: ["a", "c"] })
    })

    it("should provide correct before and after states when an object property has been removed", () => {
      const doc = Automerge.from<{ obj: { a: string; b?: string } }>({
        obj: { a: "a", b: "b" },
      })

      const newDoc = Automerge.change(
        doc,
        {
          patchCallback: (_, patchInfo) => {
            assert.deepEqual(
              patchInfo.before.obj,
              { a: "a", b: "b" },
              "before should be the original object",
            )
            assert.deepEqual(patchInfo.after.obj, { a: "a" })
          },
        },
        doc => {
          delete doc.obj.b
        },
      )

      assert.deepEqual(newDoc, { obj: { a: "a" } })
    })
  })

  describe("the diff function", () => {
    it("should return a set of patches", () => {
      const doc = Automerge.from<{ birds: string[]; fish?: string[] }>({
        birds: ["goldfinch"],
      })
      const before = Automerge.getHeads(doc)
      const newDoc = Automerge.change(doc, doc => {
        doc.birds.push("greenfinch")
        doc.fish = ["cod"] as unknown as List<string>
      })
      const after = Automerge.getHeads(newDoc)
      const patches = Automerge.diff(newDoc, before, after)
      assert.deepEqual(patches, [
        { action: "put", path: ["fish"], value: [] },
        { action: "insert", path: ["birds", 1], values: [""] },
        { action: "splice", path: ["birds", 1, 0], value: "greenfinch" },
        { action: "insert", path: ["fish", 0], values: [""] },
        { action: "splice", path: ["fish", 0, 0], value: "cod" },
      ])
    })

    it("should throw a nice exception if before or after are not an array", () => {
      let doc = Automerge.from({ text: "hello world" })
      const goodBefore = Automerge.getHeads(doc)

      doc = Automerge.change(doc, d => {
        Automerge.splice(d, ["text"], 0, 0, "hello ")
      })

      const goodAfter = Automerge.getHeads(doc)

      assert.throws(
        () => Automerge.diff(doc, null as any, goodAfter),
        /before must be an array/,
      )
      assert.throws(
        () => Automerge.diff(doc, goodBefore, null as any),
        /after must be an array/,
      )
    })
  })

  it("should correctly diff the reverse of deleting a string value on next", () => {
    const doc = Automerge.from<{ list: string[] }>({ list: ["a", "b", "c"] })

    Automerge.change(
      doc,
      {
        patchCallback: (_, patchInfo) => {
          const reverse = Automerge.diff(
            patchInfo.after,
            Automerge.getHeads(patchInfo.after),
            Automerge.getHeads(patchInfo.before),
          )
          assert.deepEqual(reverse, [
            { action: "insert", path: ["list", 1], values: [""] },
            { action: "splice", path: ["list", 1, 0], value: "b" },
          ])
        },
      },
      doc => {
        Automerge.deleteAt(doc.list, 1)
      },
    )
  })
})
