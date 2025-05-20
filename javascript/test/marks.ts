import * as assert from "assert"
import * as Automerge from "../src/index.js"

describe("Automerge", () => {
  describe("marks", () => {
    it("should allow marks that can be seen in patches", () => {
      let value = "bold"
      let callbacks: Automerge.Patch[][] = []
      let doc1 = Automerge.init<{ x: string }>({
        patchCallback: (patches, info) => callbacks.push(patches),
      })
      doc1 = Automerge.change<{ x: string }>(doc1, d => {
        d.x = "the quick fox jumps over the lazy dog"
      })
      doc1 = Automerge.change(doc1, d => {
        Automerge.mark(
          d,
          ["x"],
          { start: 5, end: 10, expand: "none" },
          "font-weight",
          value,
        )
      })

      doc1 = Automerge.change(doc1, d => {
        Automerge.unmark(d, ["x"], { start: 7, end: 9 }, "font-weight")
      })

      assert.deepStrictEqual(callbacks[1], [
        {
          action: "mark",
          path: ["x"],
          marks: [{ name: "font-weight", start: 5, end: 10, value }],
        },
      ])

      assert.deepStrictEqual(callbacks[2], [
        {
          action: "mark",
          path: ["x"],
          marks: [
            {
              name: "font-weight",
              start: 7,
              end: 9,
              value: null,
            },
          ],
        },
      ])

      callbacks = []

      let doc2 = Automerge.init({
        patchCallback: (patches, info) => callbacks.push(patches),
      })
      doc2 = Automerge.loadIncremental(doc2, Automerge.save(doc1))

      assert.deepStrictEqual(callbacks[0][2], {
        action: "splice",
        path: ["x", 5],
        value: "ui",
        marks: { "font-weight": "bold" },
      })

      assert.deepStrictEqual(Automerge.marks(doc2, ["x"]), [
        { name: "font-weight", value, start: 5, end: 7 },
        { name: "font-weight", value, start: 9, end: 10 },
      ])
    })
  })

  it("should do unicode sensibly", () => {
    let doc = Automerge.from({ content: "😀😀" })

    doc = Automerge.change(doc, d => {
      Automerge.mark(
        d,
        ["content"],
        { start: 2, end: 4, expand: "none" },
        "bold",
        true,
      )
      Automerge.splice(d, ["content"], 0, 0, "🙃")
    })
    assert.deepStrictEqual(Automerge.marks(doc, ["content"]), [
      {
        name: "bold",
        value: true,
        start: 4,
        end: 6,
      },
    ])
    doc = Automerge.change(doc, d => {
      Automerge.unmark(
        d,
        ["content"],
        { start: 4, end: 6, expand: "none" },
        "bold",
      )
    })
    assert.deepStrictEqual(Automerge.marks(doc, ["content"]), [])
  })

  // test thanks to @jjallaire
  // https://github.com/automerge/automerge/issues/646
  it("patches properly report marks on end of expand true", () => {
    let patches: Automerge.Patch[] = []
    let doc = Automerge.from(
      { text: "aaabbbccc" },
      { patchCallback: p => patches.push(...p) },
    )

    doc = Automerge.change(doc, doc => {
      Automerge.mark(
        doc,
        ["text"],
        { start: 3, end: 6, expand: "both" },
        "bold",
        true,
      )
      const marks = Automerge.marks(doc, ["text"])
      assert.deepStrictEqual(marks, [
        { name: "bold", value: true, start: 3, end: 6 },
      ])
    })

    doc = Automerge.change(doc, doc => {
      Automerge.splice(doc, ["text"], 6, 0, "<")
      Automerge.splice(doc, ["text"], 3, 0, ">")
      const marks = Automerge.marks(doc, ["text"])
      assert.deepStrictEqual(marks, [
        { name: "bold", value: true, start: 3, end: 8 },
      ])
    })

    assert.deepStrictEqual(patches.pop(), {
      action: "splice",
      path: ["text", 3],
      value: ">",
      marks: { bold: true },
    })
    assert.deepStrictEqual(patches.pop(), {
      action: "splice",
      path: ["text", 6],
      value: "<",
      marks: { bold: true },
    })

    assert.deepStrictEqual(Automerge.marksAt(doc, ["text"], 2), {}) // a
    assert.deepStrictEqual(Automerge.marksAt(doc, ["text"], 3), { bold: true }) // <
    assert.deepStrictEqual(Automerge.marksAt(doc, ["text"], 5), { bold: true }) // b
    assert.deepStrictEqual(Automerge.marksAt(doc, ["text"], 7), { bold: true }) // >
    assert.deepStrictEqual(Automerge.marksAt(doc, ["text"], 8), {}) // c
  })
})
