import * as assert from "assert"
import * as Automerge from "../src/index.js"

const CHANGE_ID = /^\d+@[0-9a-f]+$/
const HASH = /^[0-9a-f]{64}$/

describe("Automerge", () => {
  describe("auditMode", () => {
    it("is on by default", () => {
      const doc = Automerge.change(Automerge.init<any>(), d => (d.a = 1))
      assert.strictEqual(Automerge.isAuditMode(doc), true)
      assert.match(Automerge.getHeads(doc)[0], HASH)
    })

    it("names heads by change id when created without it", () => {
      let doc = Automerge.init<any>({ actor: "aaaa", auditMode: false })
      assert.strictEqual(Automerge.isAuditMode(doc), false)
      doc = Automerge.change(doc, d => (d.a = 1))
      doc = Automerge.change(doc, d => (d.b = 2))
      assert.deepStrictEqual(Automerge.getHeads(doc), ["2@aaaa"])
      assert.strictEqual(Automerge.hasHeads(doc, ["1@aaaa"]), true)
      assert.strictEqual(Automerge.hasHeads(doc, ["3@aaaa"]), false)
      assert.deepStrictEqual(Automerge.view(doc, ["1@aaaa"]), { a: 1 })
    })

    it("applies to from", () => {
      const doc = Automerge.from({ a: 1 }, { auditMode: false })
      assert.strictEqual(Automerge.isAuditMode(doc), false)
      assert.match(Automerge.getHeads(doc)[0], CHANGE_ID)
    })

    it("loads without hashes and round trips through save", () => {
      const audited = Automerge.from({ a: 1 })
      const doc = Automerge.load<any>(Automerge.save(audited), {
        auditMode: false,
      })
      assert.strictEqual(Automerge.isAuditMode(doc), false)
      assert.deepStrictEqual(doc, { a: 1 })

      const next = Automerge.change<any>(doc, d => (d.b = 2))
      const reloaded = Automerge.load<any>(Automerge.save(next))
      assert.strictEqual(Automerge.isAuditMode(reloaded), true)
      assert.deepStrictEqual(reloaded, { a: 1, b: 2 })
    })

    it("loads without hashes with a patch callback", () => {
      const bytes = Automerge.save(Automerge.from({ a: 1 }))
      let patched = false
      const doc = Automerge.load<any>(bytes, {
        auditMode: false,
        patchCallback: () => (patched = true),
      })
      assert.strictEqual(Automerge.isAuditMode(doc), false)
      assert.deepStrictEqual(doc, { a: 1 })
      assert.strictEqual(patched, true)
    })

    it("switches mode with clone and leaves the original alone", () => {
      const doc = Automerge.from({ a: 1 })
      const heads = Automerge.getHeads(doc)
      const old = Automerge.view(doc, heads)

      const next = Automerge.clone(doc, { auditMode: false })
      assert.strictEqual(Automerge.isAuditMode(next), false)
      assert.match(Automerge.getHeads(next)[0], CHANGE_ID)
      assert.deepStrictEqual(old, { a: 1 })
      assert.deepStrictEqual(Automerge.getHeads(doc), heads)

      const sameMode = Automerge.clone(next)
      assert.strictEqual(Automerge.isAuditMode(sameMode), false)

      const back = Automerge.clone(next, { auditMode: true })
      assert.deepStrictEqual(Automerge.getHeads(back), heads)
    })

    it("refuses to sync without hashes", () => {
      const doc = Automerge.from({ a: 1 }, { auditMode: false })
      assert.throws(() =>
        Automerge.generateSyncMessage(doc, Automerge.initSyncState()),
      )
    })
  })
})
