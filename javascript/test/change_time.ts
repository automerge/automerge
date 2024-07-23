import * as assert from "assert"
import {
  next as Automerge,
  decodeChange,
  getLastLocalChange,
} from "../src/index.js"

describe("Automerge", () => {
  describe("change", () => {
    it("should default to current timestamp", () => {
      let doc = Automerge.init<{ answer: number }>()
      const time = Date.now()
      doc = Automerge.change(doc, d => (d.answer = 42))
      const change = getLastLocalChange(doc)
      assert.ok(change)
      const decoded = decodeChange(change)
      assert.equal(decoded.time, time)
    })
    it("should allow user provided timestamp", () => {
      let doc = Automerge.init<{ answer: number }>()
      const time = 12345
      doc = Automerge.change(doc, { time }, d => (d.answer = 42))
      const change = getLastLocalChange(doc)
      assert.ok(change)
      const decoded = decodeChange(change)
      assert.equal(decoded.time, time)
    })
    it("should allow no timestamp", () => {
      let doc = Automerge.init<{ answer: number }>()
      doc = Automerge.change(doc, { time: undefined }, d => (d.answer = 42))
      const change = getLastLocalChange(doc)
      assert.ok(change)
      const decoded = decodeChange(change)
      assert.equal(decoded.time, 0)
    })
  })
  describe("emptyChange", () => {
    it("should default to current timestamp", () => {
      let doc = Automerge.init()
      const time = Date.now()
      doc = Automerge.emptyChange(doc)
      const change = getLastLocalChange(doc)
      assert.ok(change)
      const decoded = decodeChange(change)
      assert.equal(decoded.time, time)
    })
    it("should allow user provided timestamp", () => {
      let doc = Automerge.init()
      const time = 12345
      doc = Automerge.emptyChange(doc, { time })
      const change = getLastLocalChange(doc)
      assert.ok(change)
      const decoded = decodeChange(change)
      assert.equal(decoded.time, time)
    })
    it("should allow no timestamp", () => {
      let doc = Automerge.init()
      doc = Automerge.emptyChange(doc, { time: undefined })
      const change = getLastLocalChange(doc)
      assert.ok(change)
      const decoded = decodeChange(change)
      assert.equal(decoded.time, 0)
    })
  })
})
