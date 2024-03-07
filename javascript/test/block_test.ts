import * as assert from "assert"
import { next as Automerge } from "../src"
import * as WASM from "@automerge/automerge-wasm"
import { mismatched_heads } from "./helpers"
import { PatchSource } from "../src/types"
import { inspect } from "util"

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
        action: "splitBlock",
        path: ["text", 3],
        index: 3,
        type: "p",
        cursor: Automerge.getCursor(doc, ["text"], 3),
        parents: ["div"],
        attrs: {}
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
        { action: "splice", path: ["text", 7], value: "ADD", block },
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

  it("emits a single split patch when call diff after splitting a block", () => {
    const block = { parents: [], type: "ordered-list-item", attrs: {} }
    let doc = Automerge.from({ text: "aaa" })
    doc = Automerge.change(doc, d => {
      Automerge.splitBlock(d, ["text"], 0, { parents: [], type: "paragraph", attrs: {} })
      Automerge.updateBlock(d, ["text"], 0, { parents: [], type: "ordered-list-item", attrs: {} })
    })

    const headsBefore = Automerge.getHeads(doc)
    doc = Automerge.change(doc, d => {
      Automerge.splitBlock(d, ["text"], 3, block)
    })
    const headsAfter = Automerge.getHeads(doc)

    const diff = Automerge.diff(doc, headsBefore, headsAfter)
    assert.deepStrictEqual(diff, [
      {
        action: "splitBlock",
        path: ["text",3],
        attrs: {},
        index: 3,
        type: "ordered-list-item",
        cursor: Automerge.getCursor(doc, ["text"], 3),
        parents: []
      },
    ])
  })

  it("allows updating all blocks at once", () => {
    let doc = Automerge.from({text: ""})
    doc = Automerge.change(doc, d => {
      Automerge.splitBlock(d, ["text"], 0, { parents: [], type: "ordered-list-item", attrs: {} })
      Automerge.splice(d, ["text"], 1, 0, "first thing")
      Automerge.splitBlock(d, ["text"], 7, { parents: [], type: "ordered-list-item", attrs: {} })
      Automerge.splice(d, ["text"], 8, 0, "second thing")
    })

    doc = Automerge.change(doc, d => {
      Automerge.updateBlocks(d, ["text"], [
        { type: "paragraph", parents: [], attrs: {} },
        "the first thing",
        { type: "unordered-list-item", parents: ["ordered-list-item"], attrs: {} },
        "the second thing",
      ])
    })
    assert.deepStrictEqual(Automerge.spans(doc, ["text"]), [
      { type: "block", value: { type: "paragraph", parents: [], attrs: {} } },
      { type: "text", value: "the first thing" },
      { type: "block", value: { type: "unordered-list-item", parents: ["ordered-list-item"], attrs: {} } },
      { type: "text", value: "the second thing" },
    ])
  })

  describe("users strings instead of RawString in block attributes", () => {

    it("when loading blocks", () => {
      let doc = Automerge.from({text: ""})
      doc = Automerge.change(doc, d => {
        Automerge.splitBlock(d, ["text"], 0, { parents: [], type: "ordered-list-item", attrs: { "data-foo": "someval" } })
        Automerge.splice(d, ["text"], 1, 0, "first thing")
      })
      const block = Automerge.block(doc, ["text"], 0)
      if (!block) throw new Error("block not found")
      assert.deepStrictEqual(block.attrs, { "data-foo": "someval" })
    })

    it("when loading spans", () => {
      let doc = Automerge.from({text: ""})
      doc = Automerge.change(doc, d => {
        Automerge.splitBlock(d, ["text"], 0, { parents: [], type: "ordered-list-item", attrs: { "data-foo": "someval" } })
        Automerge.splice(d, ["text"], 1, 0, "first thing")
      })
      const spans = Automerge.spans(doc, ["text"])
      const block = spans[0]
      if (!(block.type === "block")) throw new Error("block not found")
      assert.deepStrictEqual(block.value.attrs, { "data-foo": "someval" })
    })

    it("in splitBlock patches", () => {
      let doc = Automerge.from({text: ""})
      const headsBefore = Automerge.getHeads(doc)
      doc = Automerge.change(doc, d => {
        Automerge.splitBlock(d, ["text"], 0, { parents: [], type: "ordered-list-item", attrs: { "data-foo": "someval"} })
      })
      const cursor = Automerge.getCursor(doc, ["text"], 0)
      const headsAfter = Automerge.getHeads(doc)
      const diff = Automerge.diff(doc, headsBefore, headsAfter)
      assert.deepStrictEqual(diff, [
        {
          action: "splitBlock",
          path: ["text",0],
          attrs: {"data-foo": "someval"},
          index: 0,
          type: "ordered-list-item",
          cursor,
          parents: []
        },
      ])
    })

    it("in updateBlock patches", () => {
      let doc = Automerge.from({text: ""})
      doc = Automerge.change(doc, d => {
        Automerge.splitBlock(d, ["text"], 0, { parents: [], type: "ordered-list-item", attrs: { "data-foo": "someval"} })
      })
      const headsBefore = Automerge.getHeads(doc)
      doc = Automerge.change(doc, d => {
        Automerge.updateBlock(d, ["text"], 0, { parents: [], type: "ordered-list-item", attrs: { "data-foo": "someotherval"} })
      })
      const headsAfter = Automerge.getHeads(doc)
      const diff = Automerge.diff(doc, headsBefore, headsAfter)
      assert.deepStrictEqual(diff, [
        {
          action: "updateBlock",
          path: ["text",0],
          index: 0,
          new_attrs: { "data-foo": "someotherval" },
          new_type: null,
          new_parents: null
        },
      ])
    })
  })

})
