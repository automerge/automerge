import { default as assert } from "assert"
import * as Automerge from "../src/entrypoints/fullfat_node.js"

describe("write_frontier", () => {
  const author = "07".repeat(32)

  it("hides and restores changes by author", () => {
    let doc = Automerge.init<any>({ author })
    doc = Automerge.change(doc, d => {
      d.value = "visible"
    })

    const noOp = Automerge.revealAuthor(doc, author)
    assert.deepEqual(noOp, doc)
    assert.equal(Automerge.isAuthorMasked(noOp, author), false)

    const masked = Automerge.maskAuthor(noOp, author, [])
    assert.equal(masked.value, undefined)
    assert.deepEqual(Automerge.getHeads(masked), Automerge.getHeads(doc))
    assert.equal(Automerge.isAuthorMasked(masked, author), true)

    const stillMasked = Automerge.maskAuthor(masked, author, [])
    assert.deepEqual(stillMasked, masked)

    const revealed = Automerge.revealAuthor(stillMasked, author)
    assert.equal(revealed.value, "visible")
    assert.equal(Automerge.isAuthorMasked(revealed, author), false)
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
    doc = Automerge.maskAuthor(doc, author, [])
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
    const masked = Automerge.maskAuthor(doc, author, heads)
    assert.deepEqual(masked.list, [1, 2])
    assert.deepEqual(before.list, [3, 1, 2])
    assert.strictEqual(masked.stable, before.stable)
    assert.deepEqual(callbacks, [
      { source: "maskAuthor", patches: [{ action: "del", path: ["list", 0] }] },
    ])

    // An explicit callback overrides the document callback. No earlier patches
    // should be replayed, even though maskAuthor/revealAuthor leave the heads unchanged.
    const revealed = Automerge.revealAuthor(masked, author, {
      patchCallback: (patches, info) => {
        assert.strictEqual(info.before, masked)
        assert.deepEqual(info.after.list, [3, 1, 2])
        callbacks.push({ patches, source: info.source })
      },
    })
    assert.deepEqual(revealed.list, [3, 1, 2])
    assert.strictEqual(revealed.stable, before.stable)
    assert.deepEqual(callbacks[1], {
      source: "revealAuthor",
      patches: [{ action: "insert", path: ["list", 0], values: [3] }],
    })
    assert.equal(callbacks.length, 2)

    doc = Automerge.revealAuthor(revealed, author)
    assert.equal(
      callbacks.length,
      2,
      "no-op write-frontier should not call back",
    )
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
