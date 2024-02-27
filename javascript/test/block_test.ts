import * as assert from "assert"
import { next as Automerge } from "../src"
import * as WASM from "@automerge/automerge-wasm"
import { mismatched_heads } from "./helpers"
import { PatchSource } from "../src/types"
import { inspect } from "util"

describe("Automerge", () => {
  describe("block", () => {
    it("can split a block", () => {
      const block = { parents: ["div"], type: "p" }
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
        parents: ["div"]
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
    const block = { parents: ["div"], type: "p" }
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
    const block = { parents: [], type: "ordered-list-item" }
    let doc = Automerge.from({ text: "aaa" })
    doc = Automerge.change(doc, d => {
      Automerge.splitBlock(d, ["text"], 0, { parents: [], type: "paragraph" })
      Automerge.updateBlock(d, ["text"], 0, { parents: [], type: "ordered-list-item" })
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
      Automerge.splitBlock(d, ["text"], 0, { parents: [], type: "ordered-list-item" })
      Automerge.splice(d, ["text"], 1, 0, "first thing")
      Automerge.splitBlock(d, ["text"], 7, { parents: [], type: "ordered-list-item" })
      Automerge.splice(d, ["text"], 8, 0, "second thing")
    })

    doc = Automerge.change(doc, d => {
      Automerge.updateBlocks(d, ["text"], [
        { type: "paragraph", parents: [] },
        "the first thing",
        { type: "unordered-list-item", parents: ["ordered-list-item"] },
        "the second thing",
      ])
    })
    assert.deepStrictEqual(Automerge.spans(doc, ["text"]), [
      { type: "block", value: { type: "paragraph", parents: [] } },
      { type: "text", value: "the first thing" },
      { type: "block", value: { type: "unordered-list-item", parents: ["ordered-list-item"] } },
      { type: "text", value: "the second thing" },
    ])
  })

})
