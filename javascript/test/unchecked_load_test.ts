import { default as assert } from "assert"
import * as Automerge from "../src/entrypoints/fullfat_node.js"
import { createHash } from "crypto"

type DocType = { k?: number; x?: number }

/** a saved doc with three sequential changes, plus its interior + head hashes */
function savedDoc(): {
  saved: Uint8Array
  early: string
  loadHeads: string[]
} {
  let doc = Automerge.init<DocType>()
  doc = Automerge.change(doc, d => (d.k = 0))
  const early = Automerge.getHeads(doc)[0]
  doc = Automerge.change(doc, d => (d.k = 1))
  doc = Automerge.change(doc, d => (d.k = 2))
  return { saved: Automerge.save(doc), early, loadHeads: Automerge.getHeads(doc) }
}

describe("unchecked loads (skipHashGraph)", () => {
  it("errors on pre-load history, works for load heads and new hashes, and recovers after rebuildHashGraph", () => {
    const { saved, early, loadHeads } = savedDoc()

    let doc = Automerge.load<DocType>(saved, { skipHashGraph: true })
    assert.deepEqual(Automerge.getHeads(doc), loadHeads)

    // add a few changes after the load
    doc = Automerge.change(doc, d => (d.k = 100))
    const new1 = Automerge.getHeads(doc)[0]
    doc = Automerge.change(doc, d => (d.k = 200))
    const new2 = Automerge.getHeads(doc)[0]

    // everything needing pre-load interior hashes throws
    assert.throws(() => Automerge.getAllChanges(doc), /hash graph/)
    assert.throws(() => Automerge.getChangesSince(doc, [early]), /hash graph/)
    assert.throws(
      () => Automerge.generateSyncMessage(doc, Automerge.initSyncState()),
      /hash graph/,
    )
    let other = Automerge.init<DocType>()
    other = Automerge.change(other, d => (d.x = 1))
    assert.throws(() => Automerge.merge(Automerge.clone(doc), other), /hash graph/)
    // fragment APIs need the hash graph too
    assert.throws(() => Automerge.getFragmentMetadata(doc), /hash graph/)

    // referencing the load heads or post-load hashes works
    assert.equal(Automerge.getChangesSince(doc, loadHeads).length, 2)
    assert.equal(Automerge.getChangesSince(doc, [new1]).length, 1)
    assert.equal(Automerge.getChangesSince(doc, [new2]).length, 0)
    assert.deepEqual(Automerge.getMissingDeps(doc, loadHeads), [])
    // the post-load changes are local, so the last local change is reachable
    assert.notEqual(Automerge.getLastLocalChange(doc), undefined)

    // rebuild: everything above now works, including fragments
    Automerge.rebuildHashGraph(doc)
    assert.equal(Automerge.getAllChanges(doc).length, 5)
    assert.equal(Automerge.getChangesSince(doc, [early]).length, 4)
    assert.ok(Automerge.getFragmentMetadata(doc).length > 0)
    const [, msg] = ((s) => [s, Automerge.generateSyncMessage(doc, s)[1]])(
      Automerge.initSyncState(),
    )
    assert.notEqual(msg, null)
    doc = Automerge.merge(Automerge.clone(doc), other)
    assert.equal(doc.x, 1)
  })

  it("loads a doc with a bit-flipped head unchecked, but rebuildHashGraph rejects it", () => {
    const { saved, loadHeads } = savedDoc()
    const head = loadHeads[0]

    // flip one bit in the stored head hash
    const bytes = Buffer.from(saved)
    const pos = bytes.indexOf(Buffer.from(head, "hex"))
    assert.notEqual(pos, -1, "head hash bytes present in saved doc")
    bytes[pos] ^= 0x01

    // re-derive the chunk checksum: first 4 bytes of
    // sha256(chunk_type . leb(len) . data); layout [magic 4][checksum 4]...
    const digest = createHash("sha256").update(bytes.subarray(8)).digest()
    digest.copy(bytes, 4, 0, 4)
    const flipped = new Uint8Array(bytes)

    // a checked load rejects the forged head outright
    assert.throws(() => Automerge.load(flipped))

    // an unchecked load takes the recorded heads on trust
    const doc = Automerge.load<DocType>(flipped, { skipHashGraph: true })
    assert.equal(doc.k, 2)
    assert.notDeepEqual(Automerge.getHeads(doc), [head])

    // ...but rebuilding the graph recomputes the true hashes and refuses
    assert.throws(() => Automerge.rebuildHashGraph(doc))
  })
})
