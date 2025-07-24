import * as assert from "assert"
import * as Automerge from "../src/index.js"
import { mismatched_heads } from "./helpers.js"
import { PatchSource } from "../src/types.js"
import { inspect } from "util"
import { ImmutableString } from "../src/immutable_string.js"

function pathsEqual(a: Automerge.Prop[], b: Automerge.Prop[]) {
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false
  }
  return true
}

describe("Automerge", () => {
  describe("block", () => {
    it("can split a block", () => {
      const block = { parents: ["div"], type: "p", attrs: {} }
      const callbacks: Automerge.Patch[][] = []
      const patchCallback = (p, _info) => {
        callbacks.push(p)
      }
      let doc = Automerge.from({ text: "aaabbbccc" })
      doc = Automerge.change(doc, { patchCallback }, d => {
        Automerge.splitBlock(d, ["text"], 3, block)
      })

      assert.deepStrictEqual(Automerge.block(doc, ["text"], 3), block)

      assert.deepStrictEqual(callbacks[0][0], {
        action: "insert",
        path: ["text", 3],
        values: [{}],
      })
      assert.deepStrictEqual(Automerge.spans(doc, ["text"]), [
        { type: "text", value: "aaa" },
        { type: "block", value: block },
        { type: "text", value: "bbbccc" },
      ])
      doc = Automerge.change(doc, { patchCallback }, d => {
        Automerge.splice(d, ["text"], 7, 0, "ADD")
      })
      assert.deepStrictEqual(callbacks[1], [
        { action: "splice", path: ["text", 7], value: "ADD" },
      ])
      doc = Automerge.change(doc, { patchCallback }, d => {
        Automerge.splice(d, ["text"], 0, 7, "REMOVE")
      })
      assert.deepStrictEqual(Automerge.spans(doc, ["text"]), [
        { type: "text", value: "REMOVEADDccc" },
      ])
    })
  })

  it("can join a block", () => {
    const block = { parents: ["div"], type: "p", attrs: {} }
    const callbacks: Automerge.Patch[][] = []
    const patchCallback = (p, _info) => {
      callbacks.push(p)
    }
    let doc = Automerge.from({ text: "aaabbbccc" })
    doc = Automerge.change(doc, { patchCallback }, d => {
      Automerge.splitBlock(d, ["text"], 3, block)
    })

    doc = Automerge.change(doc, { patchCallback }, d => {
      Automerge.joinBlock(d, ["text"], 3)
    })
    assert.deepStrictEqual(Automerge.spans(doc, ["text"]), [
      { type: "text", value: "aaabbbccc" },
    ])
  })

  describe("updateSpans", () => {
    it("allows updating all blocks at once", () => {
      let doc = Automerge.from({ text: "" })
      doc = Automerge.change(doc, d => {
        Automerge.splitBlock(d, ["text"], 0, {
          parents: [],
          type: "ordered-list-item",
          attrs: {},
        })
        Automerge.splice(d, ["text"], 1, 0, "first thing")
        Automerge.splitBlock(d, ["text"], 7, {
          parents: [],
          type: "ordered-list-item",
          attrs: {},
        })
        Automerge.splice(d, ["text"], 8, 0, "second thing")
      })

      doc = Automerge.change(doc, d => {
        Automerge.updateSpans(
          d,
          ["text"],
          [
            {
              type: "block",
              value: { type: "paragraph", parents: [], attrs: {} },
            },
            { type: "text", value: "the first thing" },
            {
              type: "block",
              value: {
                type: "unordered-list-item",
                parents: ["ordered-list-item"],
                attrs: {},
              },
            },
            { type: "text", value: "the second thing" },
          ],
        )
      })
      assert.deepStrictEqual(Automerge.spans(doc, ["text"]), [
        { type: "block", value: { type: "paragraph", parents: [], attrs: {} } },
        { type: "text", value: "the first thing" },
        {
          type: "block",
          value: {
            type: "unordered-list-item",
            parents: ["ordered-list-item"],
            attrs: {},
          },
        },
        { type: "text", value: "the second thing" },
      ])
    })

    it("emits insert patches with ImmutableString for attribute updatese", () => {
      let doc = Automerge.from({ text: "" })
      doc = Automerge.change(doc, d => {
        Automerge.splitBlock(d, ["text"], 0, {
          parents: [],
          type: "paragraph",
          attrs: {},
        })
      })
      const patches: Automerge.Patch[] = []
      doc = Automerge.change(
        doc,
        {
          patchCallback: p => {
            patches.push(...p)
          },
        },
        d => {
          Automerge.updateSpans(
            d,
            ["text"],
            [
              {
                type: "block",
                value: {
                  type: "paragraph",
                  parents: [new Automerge.ImmutableString("someparent")],
                  attrs: {},
                },
              },
            ],
          )
        },
      )

      assert.deepStrictEqual(patches, [
        {
          action: "insert",
          path: ["text", 0, "parents", 0],
          values: [new Automerge.ImmutableString("someparent")],
        },
      ])
    })

    it("should update marks", () => {
      let doc = Automerge.from({ text: "hello world" })
      doc = Automerge.change(doc, d => {
        Automerge.updateSpans(
          d,
          ["text"],
          [
            { type: "text", value: "hello", marks: { bold: true } },
            { type: "text", value: " " },
            { type: "text", value: " world", marks: { italic: true } },
          ],
        )
      })
      const spans = Automerge.spans(doc, ["text"])
      assert.deepStrictEqual(spans, [
        { type: "text", value: "hello", marks: { bold: true } },
        { type: "text", value: " " },
        { type: "text", value: " world", marks: { italic: true } },
      ])
    })

    it("allows configuring the default expand value of created marks", () => {
      let doc = Automerge.from({ text: "" })
      doc = Automerge.change(doc, d => {
        Automerge.updateSpans(
          d,
          ["text"],
          [
            { type: "text", value: "hello", marks: { bold: true } },
            { type: "text", value: " world" },
          ],
          { defaultExpand: "none" },
        )
      })
      // Now insert a character at the end of the span
      doc = Automerge.change(doc, d => {
        Automerge.splice(d, ["text"], 5, 0, "!")
      })
      const spans = Automerge.spans(doc, ["text"])
      // The bold span shouldn't expand because we set the defaultExpand to "none"
      assert.deepStrictEqual(spans, [
        { type: "text", value: "hello", marks: { bold: true } },
        { type: "text", value: "! world" },
      ])
    })

    it("should allow overriding the default expand on a per mark basis", () => {
      let doc = Automerge.from({ text: "" })
      doc = Automerge.change(doc, d => {
        Automerge.updateSpans(
          d,
          ["text"],
          [
            { type: "text", value: "hello", marks: { bold: true } },
            { type: "text", value: " world" },
          ],
          { defaultExpand: "none", perMarkExpand: { bold: "both" } },
        )
      })
      // Now insert a character at the end of the span
      doc = Automerge.change(doc, d => {
        Automerge.splice(d, ["text"], 5, 0, "!")
      })
      const spans = Automerge.spans(doc, ["text"])
      // The bold span should expand because we overrode the defaultExpand with "both"
      assert.deepStrictEqual(spans, [
        { type: "text", value: "hello!", marks: { bold: true } },
        { type: "text", value: " world" },
      ])
    })

    it("should allow omitting any part of the update spans config", () => {
      let doc = Automerge.from({ text: "" })
      doc = Automerge.change(doc, d => {
        Automerge.updateSpans(
          d,
          ["text"],
          [
            { type: "text", value: "hello", marks: { bold: true } },
            { type: "text", value: " world" },
          ],
          { defaultExpand: "none" }, // Only providing defaultExpand
        )
      })

      doc = Automerge.change(doc, d => {
        Automerge.updateSpans(
          d,
          ["text"],
          [
            { type: "text", value: "hello", marks: { bold: true } },
            { type: "text", value: " world" },
          ],
          { perMarkExpand: { bold: "none" } }, // Only providing perMarkExpand
        )
      })

      // no config at all
      doc = Automerge.change(doc, d => {
        Automerge.updateSpans(
          d,
          ["text"],
          [
            { type: "text", value: "hello", marks: { bold: true } },
            { type: "text", value: " world" },
          ],
        )
      })
    })
  })

  describe("allows using RawString instead of RawString in block attributes", () => {
    it("when loading blocks", () => {
      let doc = Automerge.from({ text: "" })
      doc = Automerge.change(doc, d => {
        Automerge.splitBlock(d, ["text"], 0, {
          parents: [],
          type: new ImmutableString("ordered-list-item"),
          attrs: { "data-foo": new ImmutableString("someval") },
        })
        Automerge.splice(d, ["text"], 1, 0, "first thing")
      })
      const block = Automerge.block(doc, ["text"], 0)
      if (!block) throw new Error("block not found")
      assert.deepStrictEqual(block.attrs, {
        "data-foo": new ImmutableString("someval"),
      })
    })

    it("when loading spans", () => {
      let doc = Automerge.from({ text: "" })
      doc = Automerge.change(doc, d => {
        Automerge.splitBlock(d, ["text"], 0, {
          parents: [new ImmutableString("div")],
          type: new ImmutableString("ordered-list-item"),
          attrs: { "data-foo": new ImmutableString("someval") },
        })
        Automerge.splice(d, ["text"], 1, 0, "first thing")
      })
      const spans = Automerge.spans(doc, ["text"])
      const block = spans[0]
      if (!(block.type === "block")) throw new Error("block not found")
      assert.deepStrictEqual(block.value.parents, [new ImmutableString("div")])
      assert.deepStrictEqual(block.value.attrs, {
        "data-foo": new ImmutableString("someval"),
      })
      assert.deepStrictEqual(
        block.value.type,
        new ImmutableString("ordered-list-item"),
      )
    })

    it("updates the document even if the only change was to a block attribute", () => {
      // The issue here was that when the only change was to a block attribute
      // there were no patches applied to the document, this in turn meant that
      // the logic which marks a document as stale was marking the current
      // document as stale.
      let doc = Automerge.from({ text: "" })
      doc = Automerge.change(doc, d => {
        Automerge.splitBlock(d, ["text"], 0, {
          parents: [],
          type: "paragraph",
          attrs: {},
        })
        Automerge.splice(d, ["text"], 1, 0, "item")
      })

      doc = Automerge.change(doc, d => {
        Automerge.updateSpans(
          d,
          ["text"],
          [
            {
              type: "block",
              value: {
                type: "paragraph",
                parents: ["ordered-list-item"],
                attrs: {},
              },
            },
            { type: "text", value: "item" },
          ],
        )
      })

      doc = Automerge.change(doc, d => {
        Automerge.splice(d, ["text"], 0, 1, "A")
      })
    })
  })

  describe("when using Automerge.view", () => {
    it("should show historical marks", () => {
      let doc = Automerge.from({ text: "hello world" })
      doc = Automerge.change(doc, d => {
        Automerge.mark(d, ["text"], { start: 0, end: 5 }, "bold", true)
      })
      const headsBefore = Automerge.getHeads(doc)
      doc = Automerge.change(doc, d => {
        Automerge.mark(d, ["text"], { start: 5, end: 11 }, "italic", true)
      })
      const spans = Automerge.spans(Automerge.view(doc, headsBefore), ["text"])
      assert.deepStrictEqual(spans, [
        { type: "text", value: "hello", marks: { bold: true } },
        { type: "text", value: " world" },
      ])
    })
  })

  it("can allow small values in block attributes", () => {
    // Exercise an issue where very small floating point numbers were converted
    // to 0 when stored in a block attribute
    const smallnum = 1.401298464324817e-45
    let doc = Automerge.from({ text: "" })
    doc = Automerge.change(doc, d => {
      Automerge.splitBlock(d, ["text"], 0, { smallnum })
    })
    const block = Automerge.block(doc, ["text"], 0)
    assert.equal(block?.smallnum, smallnum)
  })
})
