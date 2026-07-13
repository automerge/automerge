import { default as assert } from "assert"
import * as Automerge from "../src/entrypoints/fullfat_node.js"
import { createHash } from "crypto"

type DocType = { k?: number; x?: number }

/** a saved doc with three sequential changes, plus its head list */
function savedDoc(): { saved: Uint8Array; loadHeads: string[] } {
  let doc = Automerge.init<DocType>()
  doc = Automerge.change(doc, d => (d.k = 0))
  doc = Automerge.change(doc, d => (d.k = 1))
  doc = Automerge.change(doc, d => (d.k = 2))
  return { saved: Automerge.save(doc), loadHeads: Automerge.getHeads(doc) }
}

describe("unchecked loads (hashGraphRebuild: none)", () => {
  it("errors on unknown history, works for known hashes and fragments, and recovers after rebuildHashGraph", () => {
    // a large doc: interior changes covered by cached fragments are not
    // carried by the saved hash columns, so they stay unknown after an
    // unchecked load (small docs are now fully covered by the columns)
    let doc = Automerge.init<DocType>()
    const hashes: string[] = []
    for (let i = 0; i < 3000; i++) {
      doc = Automerge.change(doc, d => (d.k = i))
      hashes.push(Automerge.getHeads(doc)[0])
    }
    const saved = Automerge.save(doc)
    const loadHeads = Automerge.getHeads(doc)

    let mid = Automerge.load<DocType>(saved, { hashGraphRebuild: "none" })
    assert.equal(Automerge.hashGraphState(mid), "fragmentHashes")
    assert.deepEqual(Automerge.getHeads(mid), loadHeads)

    // probe for a genuinely unknown interior hash
    const backend = Automerge.getBackend(mid)
    const unknown = hashes.find(h => {
      try {
        backend.getChangeByHash(h)
        return false
      } catch (e) {
        return true
      }
    })
    assert.ok(unknown, "expected an unknown interior hash in a 3000-change doc")

    // add changes after the load
    mid = Automerge.change(mid, d => (d.k = 100_000))
    const new1 = Automerge.getHeads(mid)[0]
    mid = Automerge.change(mid, d => (d.k = 200_000))
    const new2 = Automerge.getHeads(mid)[0]

    // unknown interior history throws
    assert.throws(() => Automerge.getAllChanges(mid), /hash graph/)
    assert.throws(
      () => Automerge.getChangesSince(mid, [unknown!]),
      /hash graph/,
    )
    assert.throws(
      () => Automerge.generateSyncMessage(mid, Automerge.initSyncState()),
      /hash graph/,
    )
    let other = Automerge.init<DocType>()
    other = Automerge.change(other, d => (d.x = 1))
    assert.throws(
      () => Automerge.merge(Automerge.clone(mid), other),
      /hash graph/,
    )

    // known hashes work
    assert.equal(Automerge.getChangesSince(mid, loadHeads).length, 2)
    assert.equal(Automerge.getChangesSince(mid, [new1]).length, 1)
    assert.equal(Automerge.getChangesSince(mid, [new2]).length, 0)
    assert.deepEqual(Automerge.getMissingDeps(mid, loadHeads), [])
    assert.notEqual(Automerge.getLastLocalChange(mid), undefined)

    // fragments work in the fragment-hashes state
    const midFragments = Automerge.getFragmentMetadata(mid)
    assert.ok(midFragments.length > 0)
    assert.ok(
      Automerge.getBackend(mid).bundleFragmentMetadata(midFragments).length > 0,
    )

    // rebuild: everything works
    Automerge.rebuildHashGraph(mid)
    assert.equal(Automerge.hashGraphState(mid), "checked")
    assert.equal(Automerge.getAllChanges(mid).length, 3002)
    assert.ok(Automerge.getChangesSince(mid, [unknown!]).length > 0)
    const [, msg] = (s => [s, Automerge.generateSyncMessage(mid, s)[1]])(
      Automerge.initSyncState(),
    )
    assert.notEqual(msg, null)
    mid = Automerge.merge(Automerge.clone(mid), other)
    assert.equal(mid.x, 1)
  })

  it("a single-change doc has no hash columns: plain unchecked state", () => {
    let doc = Automerge.init<DocType>()
    doc = Automerge.change(doc, d => (d.k = 1))
    const saved = Automerge.save(doc)

    const loaded = Automerge.load<DocType>(saved, { hashGraphRebuild: "none" })
    assert.equal(Automerge.hashGraphState(loaded), "unchecked")
    assert.throws(() => Automerge.getFragmentMetadata(loaded), /hash graph/)

    Automerge.rebuildHashGraph(loaded)
    assert.equal(Automerge.hashGraphState(loaded), "checked")
    assert.equal(Automerge.getFragmentMetadata(loaded).length, 1)
  })

  it("hashGraphRebuild: 'fragments' uses stored hashes or falls back to a rebuild", () => {
    // a large doc carries hash columns: fragments mode lands in the
    // middle state with working fragment APIs, no rebuild
    let big = Automerge.init<DocType>()
    for (let i = 0; i < 3000; i++) {
      big = Automerge.change(big, d => (d.k = i))
    }
    const bigSaved = Automerge.save(big)
    const mid = Automerge.load<DocType>(bigSaved, {
      hashGraphRebuild: "fragments",
    })
    assert.equal(Automerge.hashGraphState(mid), "fragmentHashes")
    assert.ok(Automerge.getFragmentMetadata(mid).length > 0)

    // a single-change doc has no hash columns: full rebuild instead
    let small = Automerge.init<DocType>()
    small = Automerge.change(small, d => (d.k = 1))
    const smallSaved = Automerge.save(small)
    const checked = Automerge.load<DocType>(smallSaved, {
      hashGraphRebuild: "fragments",
    })
    assert.equal(Automerge.hashGraphState(checked), "checked")
    assert.equal(Automerge.getFragmentMetadata(checked).length, 1)
  })

  it("rejects an unknown hashGraphRebuild value", () => {
    const { saved } = savedDoc()
    assert.throws(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      () =>
        Automerge.load<DocType>(saved, { hashGraphRebuild: "sideways" } as any),
      /hashGraphRebuild/,
    )
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
    const doc = Automerge.load<DocType>(flipped, { hashGraphRebuild: "none" })
    assert.equal(doc.k, 2)
    assert.notDeepEqual(Automerge.getHeads(doc), [head])

    // ...but rebuilding the graph recomputes the true hashes and refuses
    assert.throws(() => Automerge.rebuildHashGraph(doc))
  })
})
