import { default as assert } from "assert"
import { webcrypto } from "node:crypto"
import * as Automerge from "../src/entrypoints/fullfat_node.js"

const subtle = webcrypto.subtle

type CounterDoc = {
  counter?: number
  title?: string
  items?: string[]
  alice?: string
  bob?: string
}

async function makeSigner() {
  const keys = (await subtle.generateKey("Ed25519", true, [
    "sign",
    "verify",
  ])) as CryptoKeyPair
  const rawPub = new Uint8Array(await subtle.exportKey("raw", keys.publicKey))
  const author = Buffer.from(rawPub).toString("hex")
  async function sign(data: Uint8Array): Promise<Uint8Array> {
    const sig = await subtle.sign("Ed25519", keys.privateKey, Buffer.from(data))
    return new Uint8Array(sig)
  }
  return { author, sign }
}

// A fixed author for receiving replicas that never sign anything: the
// compressed Ed25519 base point, a valid non-weak public key.
const RECEIVER_AUTHOR = "58" + "66".repeat(31)
const RECEIVER = { signing: true as const, author: RECEIVER_AUTHOR }

async function signRequired<T>(
  doc: Automerge.Doc<T>,
  sign: (data: Uint8Array) => Promise<Uint8Array>,
): Promise<Uint8Array[]> {
  const payloads: Uint8Array[] = []
  for (const data of Automerge.requiredSignatures(doc)) {
    payloads.push(data)
    const sig = await sign(data)
    Automerge.insertSignature(doc, data, sig)
  }
  return payloads
}

function concatBytes(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((sum, c) => sum + c.length, 0)
  const out = new Uint8Array(total)
  let offset = 0
  for (const chunk of chunks) {
    out.set(chunk, offset)
    offset += chunk.length
  }
  return out
}

describe("fragment signing API", () => {
  it("tracks locally authored commits, withholds unsigned exports, and round-trips proof-bearing fragments", async () => {
    const alice = await makeSigner()
    let doc = Automerge.from<CounterDoc>(
      { counter: 0 },
      { author: alice.author, signing: true },
    )

    let totalCommits = 1
    let hitFragment = false
    while (!hitFragment || totalCommits < 5) {
      totalCommits++
      doc = Automerge.change(doc, { time: totalCommits }, d => {
        d.counter = totalCommits
      })
      const [head] = Automerge.getHeads(doc)
      if (totalCommits >= 2 && head.startsWith("00")) {
        hitFragment = true
      }
    }
    for (let extra = 0; extra < 3; extra++) {
      totalCommits++
      doc = Automerge.change(doc, { time: totalCommits }, d => {
        d.counter = totalCommits
      })
    }

    // Local edits are visible locally, but unsigned exports are withheld.
    assert.equal(doc.counter, totalCommits)
    assert.equal(Automerge.getFragments(doc).length, 0)
    assert.equal(Automerge.getCommits(doc).length, 0)
    assert.equal(Automerge.getFragmentMetadata(doc).length, 0)

    const payloads = [...Automerge.requiredSignatures(doc)]
    assert.equal(payloads.length, totalCommits)
    for (const data of payloads) {
      assert.equal(data.length, 61)
      assert.equal(
        Buffer.from(data.subarray(0, 29)).toString("utf8"),
        "automerge-change-signature-v1",
      )
      const sig = await alice.sign(data)
      Automerge.insertSignature(doc, data, sig)
      // Duplicate insertion is idempotent; also check next namespace export.
      Automerge.next.insertSignature(doc, data, sig)
    }
    assert.deepEqual([...Automerge.requiredSignatures(doc)], [])
    assert.deepEqual([...Automerge.next.requiredSignatures(doc)], [])

    const fragments = Automerge.getFragments(doc)
    const commits = Automerge.getCommits(doc)
    assert.ok(fragments.length >= 1)
    assert.ok(commits.length >= 1)
    assert.ok(fragments.length + commits.length < totalCommits)

    // Reconstruct in a signing document via addFragments + addCommits with patch callbacks.
    const patchSources: Automerge.PatchSource[] = []
    let receiver = Automerge.init<CounterDoc>(RECEIVER)
    receiver = Automerge.addFragments(receiver, fragments, {
      patchCallback: (_patches, info) => patchSources.push(info.source),
    })
    receiver = Automerge.addCommits(receiver, commits, {
      patchCallback: (_patches, info) => patchSources.push(info.source),
    })
    assert.ok(patchSources.includes("addFragments"))
    assert.ok(patchSources.includes("addCommits"))
    assert.deepEqual(Automerge.getHeads(receiver), Automerge.getHeads(doc))
    assert.equal(receiver.counter, totalCommits)

    // Reconstruct via load(..., { signing: true }) both directly and with patchCallback.
    const wire = concatBytes(
      Automerge.bundleFragmentMetadata(doc, Automerge.getFragmentMetadata(doc)),
    )
    const loaded = Automerge.load<CounterDoc>(wire, RECEIVER)
    assert.deepEqual(Automerge.getHeads(loaded), Automerge.getHeads(doc))
    assert.equal(loaded.counter, totalCommits)

    const loadPatches: Automerge.Patch[][] = []
    const loadedWithPatches = Automerge.load<CounterDoc>(wire, {
      ...RECEIVER,
      patchCallback: patches => loadPatches.push(patches),
    })
    assert.equal(loadedWithPatches.counter, totalCommits)
    assert.ok(loadPatches.length > 0)

    // Unsigned documents can also ingest proof-bearing fragments and commits.
    let unsignedReceiver = Automerge.init<CounterDoc>()
    unsignedReceiver = Automerge.addFragments(unsignedReceiver, fragments)
    unsignedReceiver = Automerge.addCommits(unsignedReceiver, commits)
    assert.equal(unsignedReceiver.counter, totalCommits)
  })

  it("returns only payloads needing signatures from the document's current author across peers", async () => {
    const alice = await makeSigner()
    const bob = await makeSigner()

    let aliceDoc = Automerge.init<CounterDoc>({
      author: alice.author,
      signing: true,
    })
    aliceDoc = Automerge.change(aliceDoc, d => {
      d.alice = "hello from alice"
      d.items = ["a"]
    })
    await signRequired(aliceDoc, alice.sign)

    const aliceWire = concatBytes(
      Automerge.bundleFragmentMetadata(
        aliceDoc,
        Automerge.getFragmentMetadata(aliceDoc),
      ),
    )

    // A replica with a different author has nothing of its own to sign.
    const reader = Automerge.load<CounterDoc>(aliceWire, RECEIVER)
    assert.deepEqual([...Automerge.requiredSignatures(reader)], [])

    let bobDoc = Automerge.load<CounterDoc>(aliceWire, {
      author: bob.author,
      signing: true,
    })
    assert.deepEqual([...Automerge.requiredSignatures(bobDoc)], [])
    assert.equal(Automerge.getAuthor(bobDoc), bob.author)

    bobDoc = Automerge.change(bobDoc, d => {
      d.bob = "hello from bob"
      d.items?.push("b")
    })
    const [bobHead] = Automerge.getHeads(bobDoc)
    const bobPayloads = await signRequired(bobDoc, bob.sign)
    assert.equal(bobPayloads.length, 1)
    assert.equal(
      Buffer.from(bobPayloads[0].subarray(29)).toString("hex"),
      bobHead,
    )
    assert.deepEqual([...Automerge.requiredSignatures(bobDoc)], [])

    const bobWire = concatBytes(
      Automerge.bundleFragmentMetadata(
        bobDoc,
        Automerge.getFragmentMetadata(bobDoc),
      ),
    )
    aliceDoc = Automerge.loadIncremental(aliceDoc, bobWire)
    assert.deepEqual(
      Automerge.getHeads(aliceDoc).sort(),
      Automerge.getHeads(bobDoc).sort(),
    )
    assert.deepEqual(aliceDoc.items, ["a", "b"])
    assert.equal(aliceDoc.bob, "hello from bob")
  })

  it("stages out-of-order incoming groups until causal dependencies and author bindings arrive", async () => {
    const alice = await makeSigner()
    let source = Automerge.init<CounterDoc>({
      author: alice.author,
      signing: true,
    })
    source = Automerge.change(source, d => {
      d.counter = 1
    })
    source = Automerge.change(source, d => {
      d.counter = 2
    })
    source = Automerge.change(source, d => {
      d.counter = 3
    })
    await signRequired(source, alice.sign)

    const chunks = Automerge.bundleFragmentMetadata(
      source,
      Automerge.getFragmentMetadata(source),
    )
    const commits = Automerge.getCommits(source)
    const byCausalOrder = [...commits].sort((a, b) => {
      if (a.parents.length === 0) return -1
      if (b.parents.length === 0) return 1
      return b.parents.includes(a.head) ? -1 : 1
    })
    assert.equal(byCausalOrder.length, 3)

    const seenValues: Array<number | undefined> = []
    let target = Automerge.init<CounterDoc>({
      ...RECEIVER,
      patchCallback: (_patches, info) => seenValues.push(info.after.counter),
    })

    target = Automerge.addCommits(target, [byCausalOrder[2]])
    assert.deepEqual(Automerge.getHeads(target), [])
    assert.equal(target.counter, undefined)
    assert.deepEqual(seenValues, [])

    target = Automerge.addCommits(target, [byCausalOrder[1]])
    assert.deepEqual(Automerge.getHeads(target), [])
    assert.deepEqual(seenValues, [])

    target = Automerge.addCommits(target, [byCausalOrder[0]])
    assert.deepEqual(Automerge.getHeads(target), Automerge.getHeads(source))
    assert.equal(target.counter, 3)
    assert.deepEqual(seenValues, [3])

    const reversedWire = concatBytes([...chunks].reverse())
    const loadedReversed = Automerge.load<CounterDoc>(reversedWire, {
      ...RECEIVER,
    })
    assert.equal(loadedReversed.counter, 3)
  })

  it("enforces independent attached proofs, signing-mode entry points, and handle invariants", async () => {
    const alice = await makeSigner()
    const mallory = await makeSigner()

    let doc = Automerge.init<CounterDoc>({
      author: alice.author,
      signing: true,
    })
    const staleDoc = doc
    doc = Automerge.change(doc, d => {
      d.title = "v1"
    })
    const [h1] = Automerge.getHeads(doc)
    const [payload] = [...Automerge.requiredSignatures(doc)]
    assert.ok(payload)

    // Wrong key, malformed payload, bad signature length, stale doc, historical view, and nested change callback all fail.
    const wrongSig = await mallory.sign(payload)
    assert.throws(
      () => Automerge.insertSignature(doc, payload, wrongSig),
      RangeError,
    )
    assert.throws(
      () => Automerge.insertSignature(doc, new Uint8Array(61), wrongSig),
      RangeError,
    )
    assert.throws(
      () => Automerge.insertSignature(doc, payload, new Uint8Array(64)),
      RangeError,
    )

    const validSig = await alice.sign(payload)
    assert.throws(
      () => Automerge.insertSignature(staleDoc, payload, validSig),
      RangeError,
    )
    const historical = Automerge.view(doc, [h1])
    assert.throws(
      () => Automerge.insertSignature(historical, payload, validSig),
      RangeError,
    )
    Automerge.change(Automerge.clone(doc), d => {
      assert.throws(
        () =>
          Automerge.insertSignature(
            d as Automerge.Doc<CounterDoc>,
            payload,
            validSig,
          ),
        RangeError,
      )
    })

    // Signing documents always carry a usable author: construction without one,
    // or with bytes that are not an Ed25519 public key, is rejected.
    assert.throws(
      () => Automerge.init<CounterDoc>({ signing: true }),
      RangeError,
    )
    assert.throws(
      () => Automerge.init<CounterDoc>({ signing: true, author: "010203" }),
      RangeError,
    )
    assert.throws(
      () => Automerge.load<CounterDoc>(new Uint8Array(0), { signing: true }),
      RangeError,
    )
    // A forged signature over a locally authored commit is rejected.
    let other = Automerge.init<CounterDoc>(RECEIVER)
    other = Automerge.change(other, d => {
      d.title = "receiver"
    })
    const [otherHead] = Automerge.getHeads(other)
    const forgedPayload = Buffer.concat([
      Buffer.from("automerge-change-signature-v1", "utf8"),
      Buffer.from(otherHead, "hex"),
    ])
    assert.throws(
      () =>
        Automerge.insertSignature(
          other,
          new Uint8Array(forgedPayload),
          new Uint8Array(64),
        ),
      RangeError,
    )

    // Sign valid doc and test incoming/entry-point restrictions.
    Automerge.insertSignature(doc, payload, validSig)
    const snapshot = Automerge.save(doc)
    const rawChange = Automerge.getLastLocalChange(doc)!

    assert.throws(() => Automerge.load(snapshot, RECEIVER), RangeError)

    const receiver = Automerge.init<CounterDoc>(RECEIVER)
    assert.throws(
      () => Automerge.loadIncremental(receiver, snapshot),
      RangeError,
    )
    assert.throws(
      () => Automerge.loadIncremental(receiver, rawChange),
      RangeError,
    )
    assert.throws(
      () => Automerge.applyChanges(receiver, [rawChange]),
      RangeError,
    )
    assert.throws(() => Automerge.merge(receiver, doc), RangeError)

    const [signedCommit] = Automerge.getCommits(doc)
    assert.throws(
      () =>
        Automerge.addCommits(receiver, [{ ...signedCommit, bytes: rawChange }]),
      RangeError,
    )
    const tamperedBytes = new Uint8Array(signedCommit.bytes)
    tamperedBytes[tamperedBytes.length - 1] ^= 0xff
    assert.throws(
      () =>
        Automerge.addCommits(receiver, [
          { ...signedCommit, bytes: tamperedBytes },
        ]),
      RangeError,
    )
    assert.deepEqual(Automerge.getHeads(receiver), [])

    // Sync protocol is disabled for unauthenticated transport in signing mode.
    let unsignedPeer = Automerge.init<CounterDoc>({ actor: "09" })
    unsignedPeer = Automerge.change(unsignedPeer, d => {
      d.counter = 42
    })
    const [, peerMsg] = Automerge.generateSyncMessage(
      unsignedPeer,
      Automerge.initSyncState(),
    )
    assert.ok(peerMsg != null)

    const [, signingMsg] = Automerge.generateSyncMessage(
      receiver,
      Automerge.initSyncState(),
    )
    assert.equal(signingMsg, null)
    assert.throws(
      () =>
        Automerge.receiveSyncMessage(
          receiver,
          Automerge.initSyncState(),
          peerMsg,
        ),
      RangeError,
    )

    // clone() at current heads works and inherits signing mode; overriding signing or cloning a view throws.
    const cloned = Automerge.clone(doc, { author: alice.author })
    assert.deepEqual([...Automerge.requiredSignatures(cloned)], [])
    assert.throws(() => Automerge.clone(doc, { signing: false }), RangeError)
    doc = Automerge.change(doc, d => {
      d.title = "v2"
    })
    const viewAtV1 = Automerge.view(doc, [h1])
    assert.throws(() => Automerge.clone(viewAtV1), RangeError)

    // Unsigned documents reject signing-only queries/mutations.
    assert.throws(
      () => [...Automerge.requiredSignatures(unsignedPeer)],
      RangeError,
    )
    assert.throws(
      () => Automerge.insertSignature(unsignedPeer, payload, validSig),
      RangeError,
    )
  })
})
