import * as assert from "assert"
import { unstable as Automerge } from "../src"
import * as WASM from "@automerge/automerge-wasm"
import { mismatched_heads } from "./helpers"

describe("Automerge", () => {
  describe("changeAt", () => {
    it("should be able to change a doc at a prior state", () => {
      let doc1 = Automerge.init()
      doc1 = Automerge.change(doc1, d => (d.text = "aaabbbccc"))
      let heads1 = Automerge.getHeads(doc1)
      doc1 = Automerge.change(doc1, d => {
        Automerge.splice(d, ["text"], 3, 3, "BBB")
      })
      assert.deepEqual(doc1.text, "aaaBBBccc")
      doc1 = Automerge.changeAt(doc1, heads1, d => {
        assert.deepEqual(d.text, "aaabbbccc")
        Automerge.splice(d, ["text"], 2, 3, "XXX")
        assert.deepEqual(d.text, "aaXXXbccc")
      })
      assert.deepEqual(doc1.text, "aaXXXBBBccc")
    })
  })
})
