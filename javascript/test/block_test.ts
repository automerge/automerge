import * as assert from "assert"
import { unstable as Automerge } from "../src"
import * as WASM from "@automerge/automerge-wasm"
import { mismatched_heads } from "./helpers"
import { PatchSource } from "../src/types"
import { inspect } from "util"

describe("Automerge", () => {
  describe("block", () => {
    it("can split a block", () => {
      let block = { parents: ["div"], type: "p" }
      let callbacks: Automerge.Patch[][] = []
      let patchCallback: any = (p, info) => {
        callbacks.push(p)
      }
      let doc = Automerge.from({ text: "aaabbbccc" })
      doc = Automerge.change(doc, { patchCallback }, d => {
        Automerge.splitBlock(d, ["text"], 3, block)
      })
      //console.log(inspect(callbacks, { depth: null, colors: true }))
      assert.deepStrictEqual(callbacks[0], [
        { action: "insert", path: ["text", 3], values: [{}] },
        { action: "put", path: ["text", 3, "parents"], value: [] },
        { action: "put", path: ["text", 3, "type"], value: "p" },
        { action: "insert", path: ["text", 3, "parents", 0], values: ["div"] },
      ])
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
})
