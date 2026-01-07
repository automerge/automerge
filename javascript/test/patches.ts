import * as assert from "assert"
import * as Automerge from "../src/index.js"
import { Patch, type List } from "../src/index.js"

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

      let invalidInputs = [null, "", "ab", ["ab"]]

      for (const invalidInput of invalidInputs) {
        assert.throws(
          () => Automerge.diff(doc, invalidInput as any, goodAfter),
          /invalid before heads/,
        )
        assert.throws(
          () => Automerge.diff(doc, goodBefore, invalidInput as any),
          /invalid after heads/,
        )
      }
    })

    it("should allow diffing a sub-object", () => {
      let doc = Automerge.from({ a: 1, foo: { b: 1, bar: { c: 1, baz: { d: 1 } } } })
      const h1 = Automerge.getHeads(doc)
      doc = Automerge.change(doc, d => {
        d.a = 2;
        d.foo.b = 2;
        d.foo.bar.c = 2;
        d.foo.bar.baz.d = 2;
      })
      const h2 = Automerge.getHeads(doc)
      doc = Automerge.change(doc, d => {
        d.foo.bar.baz.d = 3;
      })
      const h3 = Automerge.getHeads(doc)
      doc = Automerge.change(doc, d => {
        d.a = 4;
        d.foo.b = 4;
        d.foo.bar.c = 4;
        d.foo.bar.baz = { d: 4 };
      })
      const h4 = Automerge.getHeads(doc)
      const patches1 = Automerge.diff(doc, h1, h4)
      assert.deepEqual(patches1, [
        { action: "put", path: ["a"], value: 4 },
        { action: "put", path: ["foo","b"], value: 4 },
        { action: "put", path: ["foo","bar","baz"], value: {} },
        { action: "put", path: ["foo","bar","c"], value: 4 },
        { action: "put", path: ["foo","bar","baz","d"], value: 4 },
      ]);
      const patches2 = Automerge.diffPath(doc, ["foo","bar"], h1, h4)
      assert.deepEqual(patches2, [
        { action: "put", path: ["foo","bar","baz"], value: {} },
        { action: "put", path: ["foo","bar","c"], value: 4 },
        { action: "put", path: ["foo","bar","baz","d"], value: 4 },
      ]);
      const patches2_shallow = Automerge.diffPath(doc, ["foo","bar"], h1, h4, { recursive: false })
      assert.deepEqual(patches2_shallow, [
        { action: "put", path: ["foo","bar","baz"], value: {} },
        { action: "put", path: ["foo","bar","c"], value: 4 },
      ]);
      const patches3 = Automerge.diffPath(doc, ["foo","bar","baz"], h1, h4)
      assert.deepEqual(patches3, [
        { action: "put", path: ["foo","bar","baz","d"], value: 4 },
      ]);
      const patches4 = Automerge.diffPath(doc, ["foo","bar"], h2, h3)
      assert.deepEqual(patches4, [
        { action: "put", path: ["foo","bar","baz","d"], value: 3 },
      ]);
      const patches5 = Automerge.diffPath(doc, ["foo","bar"], h3, h2)
      assert.deepEqual(patches5, [
        { action: "put", path: ["foo","bar","baz","d"], value: 2 },
      ]);
      const patches5_repeat = Automerge.diffPath(doc, ["foo","bar"], h3, h2)
      assert.deepEqual(patches5_repeat, [
        { action: "put", path: ["foo","bar","baz","d"], value: 2 },
      ]);
      const patches6 = Automerge.diffPath(doc, ["foo","bar"], [], h4)
      assert.deepEqual(patches6, [
        { action: "put", path: ["foo","bar","baz"], value: {} },
        { action: "put", path: ["foo","bar","c"], value: 4 },
        { action: "put", path: ["foo","bar","baz","d"], value: 4 },
      ]);
      const patches6_shallow = Automerge.diffPath(doc, ["foo","bar"], [], h4, {recursive: false})
      assert.deepEqual(patches6_shallow, [
        { action: "put", path: ["foo","bar","baz"], value: {} },
        { action: "put", path: ["foo","bar","c"], value: 4 },
      ]);
      const patches7 = Automerge.diffPath(doc, ["foo","bar"], h3, h4)
      assert.deepEqual(patches7, [
        { action: "put", path: ["foo","bar","baz"], value: {} },
        { action: "put", path: ["foo","bar","c"], value: 4 },
        { action: "put", path: ["foo","bar","baz","d"], value: 4 },
      ]);
      const patches7_shallow = Automerge.diffPath(doc, ["foo","bar"], h3, h4, {recursive: false})
      assert.deepEqual(patches7_shallow, [
        { action: "put", path: ["foo","bar","baz"], value: {} },
        { action: "put", path: ["foo","bar","c"], value: 4 },
      ]);
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

  it("should produce correct patches during changeAt", () => {
    // This test exercises the bug reported in https://github.com/automerge/automerge/issues/951
    //
    // The problem was that the patches emitted by changeAt erroenously included
    // patches for objects that are not visible in the final state of the document
    // which cause garbled content. This was especially difficult to track down
    // because it was only triggered by larger patches

    let doc = Automerge.init<{ name?: string; color?: string }>()

    let beginning = Automerge.getHeads(doc)

    doc = Automerge.change(doc, (d: any) => {
      d.name = "a".repeat(100) // Bug is triggered by more than 100 patches
    })

    doc = Automerge.changeAt(doc, beginning, (d: any) => {
      d.color = "red"
    }).newDoc

    doc = Automerge.changeAt(doc, beginning, (d: any) => {
      d.color = "unset"
    }).newDoc
    // The bug manifested as `doc.color` being "usetred" rather than the expected "unset"
    assert.deepStrictEqual(doc.color, "unset")
  })

  describe("the applyPatches function", () => {
    describe("when applying to an automerge document", () => {
      it("should apply a map update", () => {
        let doc = Automerge.from<{ foo: { bar: string } }>({
          foo: { bar: "baz" },
        })
        const patch: Patch = {
          action: "put",
          path: ["foo", "bar"],
          value: "qux",
        }
        doc = Automerge.change(doc, d => Automerge.applyPatches(d, [patch]))
        assert.deepStrictEqual(doc.foo.bar, "qux")
      })

      it("should apply a list update patch", () => {
        let doc = Automerge.from<{ foo: string[] }>({ foo: ["bar"] })
        const patch: Patch = {
          action: "put",
          path: ["foo", 0],
          value: "baz",
        }
        doc = Automerge.change(doc, d => Automerge.applyPatches(d, [patch]))
        assert.deepStrictEqual(doc.foo[0], "baz")
      })

      it("should apply a list insertion patch", () => {
        let doc = Automerge.from<{ foo: string[] }>({ foo: ["bar"] })
        const patch: Patch = {
          action: "insert",
          path: ["foo", 1],
          values: ["baz", "qux"],
        }
        doc = Automerge.change(doc, d => Automerge.applyPatches(d, [patch]))
        assert.deepStrictEqual(doc.foo, ["bar", "baz", "qux"])
      })

      it("should apply a list deletion patch without length", () => {
        let doc = Automerge.from<{ foo: string[] }>({
          foo: ["bar", "baz", "qux"],
        })
        const patch: Patch = {
          action: "del",
          path: ["foo", 1],
        }
        doc = Automerge.change(doc, d => Automerge.applyPatches(d, [patch]))
        assert.deepStrictEqual(doc.foo, ["bar", "qux"])
      })

      it("should apply a list deletion patch with length", () => {
        let doc = Automerge.from<{ foo: string[] }>({
          foo: ["bar", "baz", "qux"],
        })
        const patch: Patch = {
          action: "del",
          path: ["foo", 0],
          length: 2,
        }
        doc = Automerge.change(doc, d => Automerge.applyPatches(d, [patch]))
        assert.deepStrictEqual(doc.foo, ["qux"])
      })

      it("should apply a text splice patch", () => {
        let doc = Automerge.from<{ foo: string }>({ foo: "bar" })
        const patch: Patch = {
          action: "splice",
          path: ["foo", 3],
          value: "baz",
        }
        doc = Automerge.change(doc, d => Automerge.applyPatches(d, [patch]))
        assert.deepStrictEqual(doc.foo, "barbaz")
      })

      it("should apply a text deletion patch without length", () => {
        let doc = Automerge.from<{ foo: string }>({ foo: "bar" })
        const patch: Patch = {
          action: "del",
          path: ["foo", 0],
        }
        doc = Automerge.change(doc, d => Automerge.applyPatches(d, [patch]))
        assert.deepStrictEqual(doc.foo, "ar")
      })

      it("should apply a text deletion patch with length", () => {
        let doc = Automerge.from<{ foo: string }>({ foo: "bar" })
        const patch: Patch = {
          action: "del",
          path: ["foo", 0],
          length: 2,
        }
        doc = Automerge.change(doc, d => Automerge.applyPatches(d, [patch]))
        assert.deepStrictEqual(doc.foo, "r")
      })

      it("should apply an increment patch", () => {
        let doc = Automerge.from<{ foo: Automerge.Counter }>({
          foo: new Automerge.Counter(1),
        })
        const patch: Patch = {
          action: "inc",
          path: ["foo"],
          value: 2,
        }
        doc = Automerge.change(doc, d => Automerge.applyPatches(d, [patch]))
        assert.deepStrictEqual(doc.foo.value, 3)
      })

      it("should apply a mark patch", () => {
        let doc = Automerge.from<{ foo: string }>({ foo: "bar" })
        const patch: Patch = {
          action: "mark",
          path: ["foo"],
          marks: [
            {
              name: "bold",
              value: true,
              start: 0,
              end: 2,
            },
          ],
        }
        doc = Automerge.change(doc, d => Automerge.applyPatches(d, [patch]))
        const marks = Automerge.marks(doc, ["foo"])
        assert.deepStrictEqual(marks, [
          { name: "bold", value: true, start: 0, end: 2 },
        ])
      })

      it("should apply an unmark patch", () => {
        let doc = Automerge.from<{ foo: string }>({ foo: "bar" })
        doc = Automerge.change(doc, d => {
          Automerge.mark(
            d,
            ["foo"],
            { start: 0, end: 2, expand: "none" },
            "bold",
            true,
          )
        })
        const patch: Patch = {
          action: "unmark",
          path: ["foo"],
          name: "bold",
          start: 0,
          end: 2,
        }
        doc = Automerge.change(doc, d => Automerge.applyPatches(d, [patch]))
        const marks = Automerge.marks(doc, ["foo"])
        assert.deepStrictEqual(marks, [])
      })
    })

    describe("when applying to a vanilla javascript object", () => {
      it("should apply a map update to a nested map", () => {
        let doc = { foo: { bar: "baz" } }
        const patch: Patch = {
          action: "put",
          path: ["foo", "bar"],
          value: "qux",
        }
        Automerge.applyPatches(doc, [patch])
        assert.deepStrictEqual(doc.foo.bar, "qux")
      })

      it("should apply a list update patch", () => {
        let doc = { foo: ["bar"] }
        const patch: Patch = {
          action: "put",
          path: ["foo", 0],
          value: "baz",
        }
        Automerge.applyPatches(doc, [patch])
        assert.deepStrictEqual(doc.foo[0], "baz")
      })

      it("should apply a list insertion patch", () => {
        let doc = { foo: ["bar"] }
        const patch: Patch = {
          action: "insert",
          path: ["foo", 1],
          values: ["baz", "qux"],
        }
        Automerge.applyPatches(doc, [patch])
        assert.deepStrictEqual(doc.foo, ["bar", "baz", "qux"])
      })

      it("should apply a list deletion patch without length", () => {
        let doc = {
          foo: ["bar", "baz", "qux"],
        }
        const patch: Patch = {
          action: "del",
          path: ["foo", 1],
        }
        Automerge.applyPatches(doc, [patch])
        assert.deepStrictEqual(doc.foo, ["bar", "qux"])
      })

      it("should apply a list deletion patch with length", () => {
        let doc = {
          foo: ["bar", "baz", "qux"],
        }
        const patch: Patch = {
          action: "del",
          path: ["foo", 0],
          length: 2,
        }
        Automerge.applyPatches(doc, [patch])
        assert.deepStrictEqual(doc.foo, ["qux"])
      })

      it("should apply a text splice patch", () => {
        let doc = { foo: "bar" }
        const patch: Patch = {
          action: "splice",
          path: ["foo", 3],
          value: "baz",
        }
        Automerge.applyPatches(doc, [patch])
        assert.deepStrictEqual(doc.foo, "barbaz")
      })

      it("should apply a text deletion patch without length", () => {
        let doc = { foo: "bar" }
        const patch: Patch = {
          action: "del",
          path: ["foo", 0],
        }
        Automerge.applyPatches(doc, [patch])
        assert.deepStrictEqual(doc.foo, "ar")
      })

      it("should apply a text deletion patch with length", () => {
        let doc = Automerge.from<{ foo: string }>({ foo: "bar" })
        const patch: Patch = {
          action: "del",
          path: ["foo", 0],
          length: 2,
        }
        doc = Automerge.change(doc, d => Automerge.applyPatches(d, [patch]))
        assert.deepStrictEqual(doc.foo, "r")
      })

      it("should apply an increment patch", () => {
        let doc = { foo: 1 }
        const patch: Patch = {
          action: "inc",
          path: ["foo"],
          value: 2,
        }
        Automerge.applyPatches(doc, [patch])
        assert.deepStrictEqual(doc.foo, 3)
      })

      it("should ignore a mark patch", () => {
        let doc = { foo: "bar" }
        const patch: Patch = {
          action: "mark",
          path: ["foo"],
          marks: [
            {
              name: "bold",
              value: true,
              start: 0,
              end: 2,
            },
          ],
        }
        Automerge.applyPatches(doc, [patch])
      })

      it("should ignore an unmark patch", () => {
        let doc = { foo: "bar" }
        const patch: Patch = {
          action: "unmark",
          path: ["foo"],
          name: "bold",
          start: 0,
          end: 2,
        }
        Automerge.applyPatches(doc, [patch])
      })

      it("should apply a map update to a map in a list in a map in a list", () => {
        let doc = Automerge.from<{ foo: { bar: { foo: string }[] }[] }>({
          foo: [{ bar: [{ foo: "hehe" }] }],
        })
        const patch: Patch = {
          action: "put",
          path: ["foo", 0, "bar", 0, "foo"],
          value: "qux",
        }
        doc = Automerge.change(doc, d => Automerge.applyPatches(d, [patch]))
        assert.deepStrictEqual(doc.foo[0].bar[0].foo, "qux")
      })
    })
  })
})
