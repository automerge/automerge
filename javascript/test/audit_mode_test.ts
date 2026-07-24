import { default as assert } from "assert"
import * as Automerge from "../src/entrypoints/fullfat_node.js"
import { createHash } from "crypto"

type DocType = { k?: number; x?: number }

/** a saved doc with three sequential changes, plus its head list */
function savedDoc(): {
  saved: Uint8Array
  loadHeads: string[]
  loadHeadHashes: string[]
} {
  let doc = Automerge.init<DocType>()
  doc = Automerge.change(doc, d => (d.k = 0))
  doc = Automerge.change(doc, d => (d.k = 1))
  doc = Automerge.change(doc, d => (d.k = 2))
  return {
    saved: Automerge.save(doc),
    loadHeads: Automerge.getHeads(doc),
    loadHeadHashes: Automerge.getHeadHashes(doc),
  }
}

describe("audit mode", () => {
  it("defaults to disabled: freed hashes throw, retained ids work, and enableAuditMode recovers everything", () => {
    // a large doc: interior changes covered by cached fragments have
    // their hashes freed outside audit mode (small docs stay fully
    // retained: their whole history is loose commits)
    let doc = Automerge.init<DocType>()
    Automerge.enableAuditMode(doc)
    const hashes: string[] = []
    for (let i = 0; i < 3000; i++) {
      doc = Automerge.change(doc, d => (d.k = i))
      hashes.push(Automerge.changeIdToHash(doc, Automerge.getHeads(doc)[0])!)
    }
    const saved = Automerge.save(doc)
    const loadHeads = Automerge.getHeads(doc)

    let mid = Automerge.load<DocType>(saved)
    assert.equal(Automerge.auditMode(mid), false)
    assert.deepEqual(Automerge.getHeads(mid), loadHeads)

    // probe for a genuinely freed interior hash
    const backend = Automerge.getBackend(mid)
    const unknown = hashes.find(h => {
      try {
        backend.getChangeByHash(h)
        return false
      } catch (e) {
        return true
      }
    })
    assert.ok(unknown, "expected a freed interior hash in a 3000-change doc")

    // add changes after the load
    mid = Automerge.change(mid, d => (d.k = 100_000))
    const new1 = Automerge.getHeads(mid)[0]
    mid = Automerge.change(mid, d => (d.k = 200_000))
    const new2 = Automerge.getHeads(mid)[0]

    // freed interior history throws
    assert.throws(() => Automerge.getAllChanges(mid), /audit/)
    const unknownId = () => Automerge.hashToChangeId(mid, unknown!)
    assert.throws(unknownId, /audit/)
    // the sync gate is deterministic: it throws outside audit mode even
    // when the retained hashes would suffice
    assert.throws(
      () => Automerge.generateSyncMessage(mid, Automerge.initSyncState()),
      /audit/,
    )

    // merge is hash-free (it identifies changes by (actor, seq)):
    // merging from a small fresh doc works outside audit mode
    let other = Automerge.init<DocType>()
    other = Automerge.change(other, d => (d.x = 1))
    const merged = Automerge.merge(Automerge.clone(mid), other)
    assert.equal(merged.x, 1)

    // retained ids work
    assert.equal(Automerge.getChangesSince(mid, loadHeads).length, 2)
    assert.equal(Automerge.getChangesSince(mid, [new1]).length, 1)
    assert.equal(Automerge.getChangesSince(mid, [new2]).length, 0)
    assert.deepEqual(Automerge.getMissingDeps(mid, loadHeads), [])
    assert.notEqual(Automerge.getLastLocalChange(mid), undefined)

    // fragments work outside audit mode: the retained set is
    // fragment-sufficient by construction
    const midFragments = Automerge.getFragmentMetadata(mid)
    assert.ok(midFragments.length > 0)
    assert.ok(
      Automerge.getBackend(mid).bundleFragmentMetadata(midFragments).length > 0,
    )

    // enable audit mode: everything works
    Automerge.enableAuditMode(mid)
    assert.equal(Automerge.auditMode(mid), true)
    assert.equal(Automerge.getAllChanges(mid).length, 3002)
    const id = Automerge.hashToChangeId(mid, unknown!)
    assert.ok(id)
    assert.ok(Automerge.getChangesSince(mid, [id!]).length > 0)
    const [, msg] = (s => [s, Automerge.generateSyncMessage(mid, s)[1]])(
      Automerge.initSyncState(),
    )
    assert.notEqual(msg, null)

    // disabling frees the interior hashes again
    Automerge.disableAuditMode(mid)
    assert.equal(Automerge.auditMode(mid), false)
    assert.throws(() => Automerge.hashToChangeId(mid, unknown!), /audit/)
  })

  it("audit load verifies and keeps every hash", () => {
    const { saved, loadHeads } = savedDoc()
    const doc = Automerge.load<DocType>(saved, { auditMode: true })
    assert.equal(Automerge.auditMode(doc), true)
    assert.equal(Automerge.getAllChanges(doc).length, 3)
    assert.deepEqual(Automerge.getHeads(doc), loadHeads)
  })

  it("fragments work on a single-change doc without hash columns", () => {
    let doc = Automerge.init<DocType>()
    doc = Automerge.change(doc, d => (d.k = 1))
    const saved = Automerge.save(doc)

    const loaded = Automerge.load<DocType>(saved)
    assert.equal(Automerge.auditMode(loaded), false)
    assert.equal(Automerge.getFragmentMetadata(loaded).length, 1)

    Automerge.enableAuditMode(loaded)
    assert.equal(Automerge.auditMode(loaded), true)
    assert.equal(Automerge.getFragmentMetadata(loaded).length, 1)
  })

  it("converts between change ids and hashes", () => {
    let doc = Automerge.init<DocType>()
    doc = Automerge.change(doc, d => (d.k = 1))
    const [head] = Automerge.getHeads(doc)
    const [headHash] = Automerge.getHeadHashes(doc)
    assert.equal(Automerge.changeIdToHash(doc, head), headHash)
    assert.equal(Automerge.hashToChangeId(doc, headHash), head)
    assert.ok(Automerge.hasHeads(doc, [head]))
    assert.ok(!Automerge.hasHeads(doc, ["99@aabbccdd"]))
  })

  it("rejects a non-boolean auditMode value", () => {
    const { saved } = savedDoc()
    assert.throws(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      () => Automerge.load<DocType>(saved, { auditMode: "sideways" } as any),
      /auditMode/,
    )
  })

  it("loads a doc with a bit-flipped head outside audit mode, but audit rejects it", () => {
    const { saved, loadHeadHashes } = savedDoc()
    const head = loadHeadHashes[0]

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

    // an audit load rejects the forged head outright
    assert.throws(() => Automerge.load(flipped, { auditMode: true }))

    // a default load takes the recorded heads on trust
    const doc = Automerge.load<DocType>(flipped)
    assert.equal(doc.k, 2)
    assert.notDeepEqual(Automerge.getHeadHashes(doc), [head])

    // ...but enabling audit mode recomputes the true hashes and refuses
    assert.throws(() => Automerge.enableAuditMode(doc))
  })
})
