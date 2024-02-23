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

  it.only("block patches are emitted after sync messages", () => {

    let doc1 = Automerge.from({ text: "aaabbbccc" })
    let doc2 = Automerge.clone(doc1)

    const headsBefore = Automerge.getHeads(doc2)
    
    function sync() {
      let sync1 = Automerge.initSyncState()
      let sync2 = Automerge.initSyncState()
      let one_to_two: Uint8Array | null
      let two_to_one: Uint8Array | null
      let done = false
      while(!done) {
        [sync1, one_to_two] = Automerge.generateSyncMessage(doc1, sync1)
        if (one_to_two != null) {
          [doc2, sync2] = Automerge.receiveSyncMessage(doc2, sync2, one_to_two)
        }

        [sync2, two_to_one] = Automerge.generateSyncMessage(doc2, sync2)
        if (two_to_one != null) {
          [doc1, sync1] = Automerge.receiveSyncMessage(doc1, sync1, two_to_one)
        }
        if (one_to_two == null && two_to_one == null) {
          done = true
        }
      }
    }

    doc1 = Automerge.change(doc1, d => {
      Automerge.splitBlock(d, ["text"], 3, { parents: [], type: "paragraph" })
    })
    sync()

    const diff = Automerge.diff(doc2, headsBefore, Automerge.getHeads(doc2))
    assert.deepStrictEqual(diff, [{
      action: "splitBlock",
      path: ["text", 3],
      type: "paragraph",
      parents: [],
    }])
  })

})
