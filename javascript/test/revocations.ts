import { default as assert } from "assert"
import * as Automerge from "../src/entrypoints/fullfat_node.js"

describe("revocations", () => {
  const author = "07".repeat(32)

  it("hides and restores changes by author", () => {
    let doc = Automerge.init<any>({ author })
    doc = Automerge.change(doc, d => {
      d.value = "visible"
    })

    const noOp = Automerge.unrevoke(doc, author)
    assert.deepEqual(noOp, doc)

    const revoked = Automerge.revoke(noOp, author, [])
    assert.equal(revoked.value, undefined)
    assert.deepEqual(Automerge.getHeads(revoked), Automerge.getHeads(doc))

    const stillRevoked = Automerge.revoke(revoked, author, [])
    assert.deepEqual(stillRevoked, revoked)

    const unrevoked = Automerge.unrevoke(stillRevoked, author)
    assert.equal(unrevoked.value, "visible")
  })

  it("invalidates cached historical patches even when the current view is unchanged", () => {
    let doc = Automerge.init<{ x?: number }>({ author })
    doc = Automerge.change(doc, d => {
      d.x = 1
    })
    const before = Automerge.getHeads(doc)
    doc = Automerge.change(doc, d => {
      delete d.x
    })
    const after = Automerge.getHeads(doc)
    assert.deepEqual(Automerge.diff(doc, before, after), [
      { action: "del", path: ["x"] },
    ])

    // There are no current-view patches, but both historical states become
    // empty. The mostRecentPatch cache must not retain the earlier deletion.
    doc = Automerge.revoke(doc, author, [])
    assert.deepEqual(doc, {})
    assert.deepEqual(Automerge.getHeads(doc), after)
    assert.deepEqual(Automerge.diff(doc, before, after), [])
  })

  it("uses incremental patches for callbacks and subsequent changes", () => {
    type Doc = { list: number[]; stable: { value: number } }
    const callbacks: { patches: Automerge.Patch[]; source: string }[] = []
    let doc = Automerge.from<Doc>(
      { list: [1, 2], stable: { value: 0 } },
      { author: "08".repeat(32) },
    )
    const heads = Automerge.getHeads(doc)
    doc = Automerge.clone(doc, {
      author,
      patchCallback: (patches, info) => {
        callbacks.push({ patches, source: info.source })
      },
    })
    doc = Automerge.change(doc, d => {
      d.list.unshift(3)
    })
    callbacks.length = 0

    const before = doc
    const revoked = Automerge.revoke(doc, author, heads)
    assert.deepEqual(revoked.list, [1, 2])
    assert.deepEqual(before.list, [3, 1, 2])
    assert.strictEqual(revoked.stable, before.stable)
    assert.deepEqual(callbacks, [
      { source: "revoke", patches: [{ action: "del", path: ["list", 0] }] },
    ])

    // An explicit callback overrides the document callback. No earlier patches
    // should be replayed, even though revoke/unrevoke leave the heads unchanged.
    const unrevoked = Automerge.unrevoke(revoked, author, {
      patchCallback: (patches, info) => {
        assert.strictEqual(info.before, revoked)
        assert.deepEqual(info.after.list, [3, 1, 2])
        callbacks.push({ patches, source: info.source })
      },
    })
    assert.deepEqual(unrevoked.list, [3, 1, 2])
    assert.strictEqual(unrevoked.stable, before.stable)
    assert.deepEqual(callbacks[1], {
      source: "unrevoke",
      patches: [{ action: "insert", path: ["list", 0], values: [3] }],
    })
    assert.equal(callbacks.length, 2)

    doc = Automerge.unrevoke(unrevoked, author)
    assert.equal(callbacks.length, 2, "no-op revocation should not call back")
    doc = Automerge.change(doc, d => {
      d.list.push(4)
    })
    assert.deepEqual(doc.list, [3, 1, 2, 4])
    assert.deepEqual(callbacks[2], {
      source: "change",
      patches: [{ action: "insert", path: ["list", 3], values: [4] }],
    })
    assert.equal(callbacks.length, 3)
  })
})
