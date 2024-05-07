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

<<<<<<< HEAD
  describe("the diff with attribution function", () => {
    it("it should be able to diff with attributes", () => {
      let doc1 = Automerge.from({ text: "hello world" }, { actor: "bbbb" })

      let heads1 = Automerge.getHeads(doc1)

      let doc2 = Automerge.clone(doc1, { actor: "aaaa" })

      doc2 = Automerge.change(doc2, d =>
        Automerge.splice(d, ["text"], 5, 1, " xxx "),
      )

      doc1 = Automerge.change(doc1, d =>
        Automerge.splice(d, ["text"], 5, 1, " yyy "),
      )

      doc1 = Automerge.merge(doc1, doc2)

      assert.deepStrictEqual(doc1.text, "hello yyy  xxx world")

      let heads2 = Automerge.getHeads(doc1)

      let actor1 = Automerge.getActorId(doc1)
      let actor2 = Automerge.getActorId(doc2)
      let attr2 = { [actor1]: "user1", [actor2]: "user2" }
      let attr3 = { [actor1]: "user1" }
      let attr4 = { [actor1]: "user1", [actor2]: "user1" }

      let patches1 = Automerge.diff(doc1, heads1, heads2)
      let patches2 = Automerge.diffWithAttribution(doc1, heads1, heads2, attr2)
      let patches3 = Automerge.diffWithAttribution(doc1, heads1, heads2, attr3)
      let patches4 = Automerge.diffWithAttribution(doc1, heads1, heads2, attr4)

      assert.deepStrictEqual(patches1, [
        { action: "splice", path: ["text", 5], value: " yyy  xxx " },
        { action: "del", path: ["text", 15], removed: " " },
      ])

      assert.deepStrictEqual(patches2, [
        { action: "splice", path: ["text", 5], value: " yyy ", attr: "user1" },
        { action: "splice", path: ["text", 10], value: " xxx ", attr: "user2" },
        { action: "del", path: ["text", 15], attr: "user1", removed: " " },
      ])

      assert.deepStrictEqual(patches3, [
        { action: "splice", path: ["text", 5], value: " yyy ", attr: "user1" },
        {
          action: "splice",
          path: ["text", 10],
          value: " xxx ",
          attr: undefined,
        },
        { action: "del", path: ["text", 15], attr: "user1", removed: " " },
      ])
    })
=======
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
>>>>>>> main
  })
})
