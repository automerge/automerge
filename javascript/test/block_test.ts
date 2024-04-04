import * as assert from "assert"
import { next as Automerge } from "../src"
import * as WASM from "@automerge/automerge-wasm"
import { mismatched_heads } from "./helpers"
import { PatchSource } from "../src/types"
import { inspect } from "util"
import { RawString } from "../src/raw_string"

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

  describe("allows using RawString instead of RawString in block attributes", () => {
    it("when loading blocks", () => {
      let doc = Automerge.from({ text: "" })
      doc = Automerge.change(doc, d => {
        Automerge.splitBlock(d, ["text"], 0, {
          parents: [],
          type: new RawString("ordered-list-item"),
          attrs: { "data-foo": new RawString("someval") },
        })
        Automerge.splice(d, ["text"], 1, 0, "first thing")
      })
      const block = Automerge.block(doc, ["text"], 0)
      if (!block) throw new Error("block not found")
      assert.deepStrictEqual(block.attrs, { "data-foo": new RawString("someval") })
    })

    it("when loading spans", () => {
      let doc = Automerge.from({ text: "" })
      doc = Automerge.change(doc, d => {
        Automerge.splitBlock(d, ["text"], 0, {
          parents: [new RawString("div")],
          type: new RawString("ordered-list-item"),
          attrs: { "data-foo": new RawString("someval") },
        })
        Automerge.splice(d, ["text"], 1, 0, "first thing")
      })
      const spans = Automerge.spans(doc, ["text"])
      const block = spans[0]
      if (!(block.type === "block")) throw new Error("block not found")
      assert.deepStrictEqual(block.value.parents, [new RawString("div")])
      assert.deepStrictEqual(block.value.attrs, { "data-foo": new RawString("someval") })
      assert.deepStrictEqual(block.value.type,  new RawString("ordered-list-item"))
    })
  })
})
