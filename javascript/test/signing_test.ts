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
  draft?: string
  mine?: string
}

const SIGNING_PREFIX = "automerge-change-signature-v1"

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

async function signRequests(
  requests: Automerge.SigningRequest[],
  sign: (data: Uint8Array) => Promise<Uint8Array>,
): Promise<Automerge.SignatureInput[]> {
  const inputs: Automerge.SignatureInput[] = []
  for (const request of requests) {
    inputs.push({
      message: request.message,
      signature: await sign(request.message),
    })
  }
  return inputs
}

/** Sign every outstanding request and insert the batch, expecting completion. */
async function publishAll<T>(
  doc: Automerge.Doc<T>,
  sign: (data: Uint8Array) => Promise<Uint8Array>,
  opts?: Automerge.ApplyOptions<T>,
): Promise<Automerge.Doc<T>> {
  const requests = Automerge.requestSignatures(doc)
  const inputs = await signRequests(requests, sign)
  const { doc: next, outcome } = Automerge.insertSignatures(doc, inputs, opts)
  assert.equal(outcome.status, "completed", JSON.stringify(outcome))
  return next
}

function completed(
  outcome: Automerge.SignatureBatchOutcome,
): Automerge.SignatureInsertion[] {
  assert.equal(outcome.status, "completed", JSON.stringify(outcome))
  if (outcome.status !== "completed") throw new Error("unreachable")
  return outcome.insertions
}

function messageFor(hash: string): Uint8Array {
  return new Uint8Array(
    Buffer.concat([
      Buffer.from(SIGNING_PREFIX, "utf8"),
      Buffer.from(hash, "hex"),
    ]),
  )
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
    assert.equal(Automerge.hasUnpublishedChanges(doc), true)

    // Requests cover canonical frontiers only (fewer than the commits) and
    // carry the exact message to sign.
    const requests = Automerge.requestSignatures(doc)
    assert.ok(requests.length >= 1)
    assert.ok(requests.length < totalCommits)
    for (const request of requests) {
      assert.equal(request.author, alice.author)
      assert.equal(request.message.length, 61)
      assert.equal(
        Buffer.from(request.message.subarray(0, 29)).toString("utf8"),
        SIGNING_PREFIX,
      )
      assert.equal(
        Buffer.from(request.message.subarray(29)).toString("hex"),
        request.hash,
      )
    }
    // Views and the `next` namespace see the same requests.
    assert.deepEqual(Automerge.next.requestSignatures(doc), requests)

    const inputs = await signRequests(requests, alice.sign)
    inputs.push(inputs[0]) // duplicate gets its own owned result
    const preBatch = doc
    const { doc: published, outcome } = Automerge.insertSignatures(doc, inputs)
    const insertions = completed(outcome)
    assert.equal(insertions.length, inputs.length)
    insertions.slice(0, requests.length).forEach((insertion, i) => {
      assert.equal(insertion.hash, requests[i].hash)
      assert.deepEqual(insertion.result, { ok: true, insert: "stored" })
      assert.deepEqual(insertion.signature, inputs[i].signature)
      assert.deepEqual(insertion.message, inputs[i].message)
    })
    assert.deepEqual(insertions[insertions.length - 1].result, {
      ok: true,
      insert: "duplicate",
    })
    // The returned document supersedes the input; the input is outdated.
    doc = published
    assert.throws(() => Automerge.change(preBatch, () => {}), RangeError)
    assert.deepEqual(Automerge.requestSignatures(doc), [])
    assert.equal(Automerge.hasUnpublishedChanges(doc), false)
    assert.equal(doc.counter, totalCommits)

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
    assert.equal(Automerge.hasUnpublishedChanges(receiver), false)
    // Imported history is never signing work for the receiver.
    assert.deepEqual(Automerge.requestSignatures(receiver), [])

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

    // Unsigned documents never silently discard attached proofs: signed
    // fragments and commits are rejected rather than stripped.
    const unsignedReceiver = Automerge.init<CounterDoc>()
    assert.throws(
      () => Automerge.addFragments(unsignedReceiver, fragments),
      RangeError,
    )
    assert.throws(
      () => Automerge.addCommits(unsignedReceiver, commits),
      RangeError,
    )
    assert.deepEqual(Automerge.getHeads(unsignedReceiver), [])
  })

  it("returns owned snapshot requests for the fixed author across peers and accepts stale responses", async () => {
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
    aliceDoc = await publishAll(aliceDoc, alice.sign)

    const aliceWire = concatBytes(
      Automerge.bundleFragmentMetadata(
        aliceDoc,
        Automerge.getFragmentMetadata(aliceDoc),
      ),
    )

    // A replica with a different author has nothing of its own to sign.
    const reader = Automerge.load<CounterDoc>(aliceWire, RECEIVER)
    assert.deepEqual(Automerge.requestSignatures(reader), [])

    let bobDoc = Automerge.load<CounterDoc>(aliceWire, {
      author: bob.author,
      signing: true,
    })
    assert.deepEqual(Automerge.requestSignatures(bobDoc), [])
    assert.equal(Automerge.getAuthor(bobDoc), bob.author)

    bobDoc = Automerge.change(bobDoc, d => {
      d.bob = "hello from bob"
      d.items?.push("b")
    })
    const [bobHead] = Automerge.getHeads(bobDoc)
    const stale = Automerge.requestSignatures(bobDoc)
    assert.equal(stale.length, 1)
    assert.equal(stale[0].hash, bobHead)
    assert.equal(stale[0].author, bob.author)

    // The snapshot is independent of later edits, and a late response to it
    // is still accepted for its exact hash.
    bobDoc = Automerge.change(bobDoc, d => {
      d.items?.push("c")
    })
    assert.equal(stale[0].hash, bobHead)
    assert.equal(Automerge.requestSignatures(bobDoc).length, 2)
    const { doc: afterStale, outcome } = Automerge.insertSignatures(
      bobDoc,
      await signRequests(stale, bob.sign),
    )
    bobDoc = afterStale
    assert.deepEqual(
      completed(outcome).map(i => i.result),
      [{ ok: true, insert: "stored" }],
    )
    assert.equal(Automerge.hasUnpublishedChanges(bobDoc), true)
    assert.equal(Automerge.requestSignatures(bobDoc).length, 1)
    bobDoc = await publishAll(bobDoc, bob.sign)
    assert.equal(Automerge.hasUnpublishedChanges(bobDoc), false)

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
    assert.deepEqual(aliceDoc.items, ["a", "b", "c"])
    assert.equal(aliceDoc.bob, "hello from bob")
    // Imported peers' history is verified but never requested locally.
    assert.deepEqual(Automerge.requestSignatures(aliceDoc), [])
  })

  it("rejects a malformed batch atomically, continues past per-entry errors, and keeps the outdated document usable on rejection", async () => {
    const alice = await makeSigner()
    const mallory = await makeSigner()

    let doc = Automerge.init<CounterDoc>({
      author: alice.author,
      signing: true,
    })
    doc = Automerge.change(doc, d => {
      d.title = "v1"
    })
    doc = Automerge.change(doc, d => {
      d.title = "v2"
    })
    const [h2] = Automerge.getHeads(doc)
    const requests = Automerge.requestSignatures(doc)
    assert.equal(requests.length, 2)
    const good = await signRequests(requests, alice.sign)

    const malformed: Array<[unknown, RegExp]> = [
      [
        [
          good[0],
          { message: new Uint8Array(61), signature: good[1].signature },
        ],
        /index 1/,
      ],
      [
        [
          good[0],
          {
            message: good[1].message.subarray(0, 40),
            signature: good[1].signature,
          },
        ],
        /index 1/,
      ],
      [
        [good[0], { message: good[1].message, signature: new Uint8Array(63) }],
        /index 1/,
      ],
      [[good[0], { message: "text", signature: good[1].signature }], /index 1/],
      [[good[0], 7], /index 1/],
      ["not an array", /array/],
    ]
    for (const [inputs, reason] of malformed) {
      const result = Automerge.insertSignatures(
        doc,
        inputs as Automerge.SignatureInput[],
      )
      assert.equal(result.outcome.status, "rejected")
      if (result.outcome.status === "rejected") {
        assert.match(result.outcome.reason, reason)
      }
      // Rejection returns the very same document; it is not outdated.
      assert.strictEqual(result.doc, doc)
    }
    assert.equal(Automerge.requestSignatures(doc).length, 2)
    assert.equal(Automerge.getCommits(doc).length, 0)
    doc = Automerge.change(doc, d => {
      d.title = "v3"
    })

    // Stale document, historical view, and nested change callback all fail.
    const stale = Automerge.init<CounterDoc>({
      author: alice.author,
      signing: true,
    })
    Automerge.change(stale, d => {
      d.title = "x"
    })
    assert.throws(() => Automerge.insertSignatures(stale, good), RangeError)
    const historical = Automerge.view(doc, [h2])
    assert.throws(
      () => Automerge.insertSignatures(historical, good),
      RangeError,
    )
    Automerge.change(Automerge.clone(doc), d => {
      assert.throws(
        () => Automerge.insertSignatures(d as Automerge.Doc<CounterDoc>, good),
        RangeError,
      )
    })

    // Per-entry errors: wrong key and unknown hash fail individually while
    // later valid entries, including a duplicate, continue.
    const current = Automerge.requestSignatures(doc)
    assert.equal(current.length, 3)
    const unknown = messageFor("ee".repeat(32))
    const wrongKey = await mallory.sign(current[0].message)
    const valid = await signRequests(current, alice.sign)
    const { doc: next, outcome } = Automerge.insertSignatures(doc, [
      { message: current[0].message, signature: wrongKey },
      { message: unknown, signature: await alice.sign(unknown) },
      ...valid,
      valid[2],
    ])
    doc = next
    const insertions = completed(outcome)
    assert.equal(insertions.length, 6)
    assert.equal(insertions[0].result.ok, false)
    assert.equal(insertions[0].hash, current[0].hash)
    assert.match(
      (insertions[0].result as { error: string }).error,
      /verification/,
    )
    assert.equal(insertions[1].result.ok, false)
    assert.equal(insertions[1].hash, "ee".repeat(32))
    assert.deepEqual(insertions[2].result, { ok: true, insert: "stored" })
    assert.deepEqual(insertions[3].result, { ok: true, insert: "stored" })
    assert.deepEqual(insertions[4].result, { ok: true, insert: "stored" })
    assert.deepEqual(insertions[5].result, { ok: true, insert: "duplicate" })
    assert.deepEqual(Automerge.requestSignatures(doc), [])
    assert.equal(Automerge.hasUnpublishedChanges(doc), false)
    assert.equal(doc.title, "v3")

    // An empty batch completes with no insertions and the document stays usable.
    const empty = Automerge.insertSignatures(doc, [])
    assert.deepEqual(empty.outcome, { status: "completed", insertions: [] })
    doc = Automerge.change(empty.doc, d => {
      d.title = "v4"
    })
    assert.equal(doc.title, "v4")

    // Unsigned documents: rejected batch, thrown request, false status.
    let unsigned = Automerge.init<CounterDoc>({ actor: "09" })
    unsigned = Automerge.change(unsigned, d => {
      d.counter = 42
    })
    const rejected = Automerge.insertSignatures(unsigned, good)
    assert.equal(rejected.outcome.status, "rejected")
    assert.strictEqual(rejected.doc, unsigned)
    assert.throws(() => Automerge.requestSignatures(unsigned), RangeError)
    assert.equal(Automerge.hasUnpublishedChanges(unsigned), false)
    unsigned = Automerge.change(unsigned, d => {
      d.counter = 43
    })
    assert.equal(unsigned.counter, 43)
  })

  it("preflights rejected batches and merge mismatches without closing pending operations", async () => {
    // Ordinary JS workflows commit before these wrappers run, but the
    // promise is stronger: a rejected batch or a mode mismatch must not
    // touch the handle at all, not even by closing its pending transaction.
    const alice = await makeSigner()
    let signed = Automerge.init<CounterDoc>({
      author: alice.author,
      signing: true,
      actor: "01",
    })
    signed = Automerge.change(signed, d => {
      d.counter = 1
    })
    const [h1] = Automerge.getHeads(signed)
    const [request] = Automerge.requestSignatures(signed)
    const good = {
      message: request.message,
      signature: await alice.sign(request.message),
    }
    let unsigned = Automerge.init<CounterDoc>({ actor: "09" })
    unsigned = Automerge.change(unsigned, d => {
      d.counter = 9
    })

    const handle = Automerge.getBackend(signed)
    handle.put("/", "title", "pending")
    assert.equal(handle.pendingOps(), 1)

    const rejected = Automerge.insertSignatures(signed, [
      good,
      { message: new Uint8Array(61), signature: good.signature },
    ])
    assert.equal(rejected.outcome.status, "rejected")
    assert.strictEqual(rejected.doc, signed)
    assert.equal(handle.pendingOps(), 1, "rejection left the transaction open")
    assert.equal(handle.getCommittedHeads()[0], h1)
    assert.throws(() => Automerge.merge(signed, unsigned), RangeError)
    assert.equal(handle.pendingOps(), 1, "mismatch left the transaction open")
    // The pending operation never became committed history.
    assert.equal(Automerge.hasUnpublishedChanges(signed), true)
    assert.deepEqual(handle.getCommittedHeads(), [h1])
    assert.equal(handle.pendingOps(), 1)

    const unsignedHandle = Automerge.getBackend(unsigned)
    unsignedHandle.put("/", "title", "pending")
    assert.equal(unsignedHandle.pendingOps(), 1)
    const wrongMode = Automerge.insertSignatures(unsigned, [good])
    assert.equal(wrongMode.outcome.status, "rejected")
    assert.strictEqual(wrongMode.doc, unsigned)
    assert.equal(unsignedHandle.pendingOps(), 1)
    assert.throws(() => Automerge.merge(unsigned, signed), RangeError)
    assert.equal(unsignedHandle.pendingOps(), 1)

    // Documents remain current: the pending edit is committed by the next
    // regular operation, then the good signature publishes normally.
    handle.rollback()
    unsignedHandle.rollback()
    const { doc: next, outcome } = Automerge.insertSignatures(signed, [good])
    assert.deepEqual(completed(outcome)[0].result, {
      ok: true,
      insert: "stored",
    })
    assert.equal(Automerge.hasUnpublishedChanges(next), false)
    assert.equal(next.title, undefined)
  })

  it("reports publicationFailed when local publication enables equivocating pending groups and keeps the document consistent", async () => {
    // Source holds unpublished draft `a`. Two same-author clones share the
    // SAME remote actor and each commit a different successor of `a`; both
    // signed level-0 groups wait behind `a` and conflict once it publishes.
    const alice = await makeSigner()
    let source = Automerge.init<CounterDoc>({
      author: alice.author,
      signing: true,
      actor: "01",
    })
    source = Automerge.change(source, d => {
      d.counter = 1
    })
    const [a] = Automerge.getHeads(source)

    const branch = async (value: string) => {
      let clone = Automerge.clone(source, { author: alice.author, actor: "02" })
      clone = Automerge.change(clone, d => {
        d.title = value
      })
      const [b] = Automerge.getHeads(clone)
      // The same-author clone is asked for `a` too; sign only `b`.
      const onlyB = Automerge.requestSignatures(clone).filter(r => r.hash === b)
      assert.equal(onlyB.length, 1)
      const inserted = Automerge.insertSignatures(
        clone,
        await signRequests(onlyB, alice.sign),
      )
      completed(inserted.outcome)
      clone = inserted.doc
      const meta = Automerge.getFragmentMeta(clone, b)
      assert.ok(meta)
      const [wire] = Automerge.bundleFragmentMetadata(clone, [meta])
      assert.ok(wire)
      assert.equal(Automerge.hasUnpublishedChanges(clone), true)
      return { clone, b, wire }
    }
    const left = await branch("left")
    const right = await branch("right")
    assert.notEqual(left.b, right.b)

    // Receiver retains both groups pending behind its unpublished `a`.
    source = Automerge.loadIncremental(source, left.wire)
    source = Automerge.loadIncremental(source, right.wire)
    assert.deepEqual(Automerge.getHeads(source), [a])
    const requests = Automerge.requestSignatures(source)
    assert.deepEqual(
      requests.map(r => r.hash),
      [a],
    )

    const seen: Automerge.PatchSource[] = []
    const before = source
    const { doc: after, outcome } = Automerge.insertSignatures(
      source,
      await signRequests(requests, alice.sign),
      { patchCallback: (_p, info) => seen.push(info.source) },
    )
    assert.equal(outcome.status, "publicationFailed", JSON.stringify(outcome))
    if (outcome.status !== "publicationFailed") throw new Error("unreachable")
    assert.equal(outcome.insertions.length, 1)
    assert.equal(outcome.insertions[0].hash, a)
    assert.deepEqual(outcome.insertions[0].result, {
      ok: true,
      insert: "stored",
    })
    assert.match(outcome.error, /admission/)
    // No conflict was applied, so there is nothing to report via patches.
    assert.deepEqual(seen, [])

    // The returned doc supersedes the input like a completed batch does.
    assert.notStrictEqual(after, before)
    assert.throws(
      () => Automerge.change(before, d => void (d.counter = 2)),
      /out of date|outdated/,
    )
    assert.equal(Automerge.hasUnpublishedChanges(after), false)
    assert.deepEqual(Automerge.getHeads(after), [a])
    assert.equal(after.counter, 1)
    assert.equal(after.title, undefined)
    assert.deepEqual(Automerge.requestSignatures(after), [])
    const reloaded = Automerge.load<CounterDoc>(Automerge.save(after), RECEIVER)
    assert.deepEqual(Automerge.getHeads(reloaded), [a])

    // Retrying without new evidence fails stably and still picks no winner.
    const retry = Automerge.insertSignatures(after, [])
    assert.equal(retry.outcome.status, "publicationFailed")
    assert.deepEqual(Automerge.getHeads(retry.doc), [a])
    source = Automerge.change(retry.doc, d => {
      d.counter = 2
    })
    assert.equal(source.counter, 2)

    // Branch documents are untouched.
    assert.deepEqual(Automerge.getHeads(left.clone), [left.b])
    assert.deepEqual(Automerge.getHeads(right.clone), [right.b])
    assert.equal(left.clone.title, "left")
    assert.equal(right.clone.title, "right")
  })

  it("delivers incoming patches from completion and keeps status pure", async () => {
    const alice = await makeSigner()
    const bob = await makeSigner()

    // Alice commits `a`. A same-author clone taken now keeps `a` as an
    // unpublished draft while the original publishes it for Bob.
    let aliceDoc = Automerge.init<CounterDoc>({
      author: alice.author,
      signing: true,
      actor: "01",
    })
    aliceDoc = Automerge.change(aliceDoc, d => {
      d.counter = 1
    })
    const [a] = Automerge.getHeads(aliceDoc)
    const laggard = Automerge.clone(aliceDoc)
    aliceDoc = await publishAll(aliceDoc, alice.sign)

    // Bob builds and publishes `b` on top of the published `a`.
    let bobDoc = Automerge.load<CounterDoc>(
      concatBytes(
        Automerge.bundleFragmentMetadata(
          aliceDoc,
          Automerge.getFragmentMetadata(aliceDoc),
        ),
      ),
      { signing: true, author: bob.author, actor: "02" },
    )
    bobDoc = Automerge.change(bobDoc, d => {
      d.bob = "dependent"
    })
    bobDoc = await publishAll(bobDoc, bob.sign)
    const [b] = Automerge.getHeads(bobDoc)
    const bobCommit = Automerge.getCommits(bobDoc).find(c => c.head === b)!
    assert.deepEqual(bobCommit.parents, [a])

    // Deliver Bob's signed commit to the laggard before its `a` is published:
    // it is verified but stays pending.
    const patches: Automerge.PatchInfo<CounterDoc>[] = []
    aliceDoc = Automerge.addCommits(laggard, [bobCommit], {
      patchCallback: (_p, info) => patches.push(info),
    })
    assert.deepEqual(Automerge.getHeads(aliceDoc), [a])
    assert.equal(aliceDoc.bob, undefined)
    assert.equal(patches.length, 0)
    // Pending incoming groups are not unpublished *applied* history, but `a` is.
    assert.equal(Automerge.hasUnpublishedChanges(aliceDoc), true)
    assert.equal(Automerge.requestSignatures(aliceDoc).length, 1)

    // Publishing `a` completes Bob's group with a patch from insertSignatures.
    const requests = Automerge.requestSignatures(aliceDoc)
    const sources: Automerge.PatchSource[] = []
    const { doc: next, outcome } = Automerge.insertSignatures(
      aliceDoc,
      await signRequests(requests, alice.sign),
      {
        patchCallback: (p, info) => {
          patches.push(info)
          sources.push(info.source)
          assert.ok(p.length > 0)
        },
      },
    )
    completed(outcome)
    aliceDoc = next
    assert.deepEqual(sources, ["insertSignatures"])
    assert.equal(patches.length, 1)
    assert.equal(patches[0].after.bob, "dependent")
    assert.deepEqual(Automerge.getHeads(aliceDoc), [b])
    assert.equal(aliceDoc.bob, "dependent")
    assert.equal(Automerge.hasUnpublishedChanges(aliceDoc), false)
    assert.deepEqual(Automerge.requestSignatures(aliceDoc), [])
  })

  it("merges published history only, in both signed directions, and rejects mode mismatches before mutation", async () => {
    const alice = await makeSigner()
    const bob = await makeSigner()
    let source = Automerge.init<CounterDoc>({
      author: alice.author,
      signing: true,
      actor: "01",
    })
    source = Automerge.change(source, d => {
      d.counter = 1
    })
    source = await publishAll(source, alice.sign)
    const [published] = Automerge.getHeads(source)
    source = Automerge.change(source, d => {
      d.draft = "unpublished"
    })
    const [draft] = Automerge.getHeads(source)

    let receiver = Automerge.init<CounterDoc>({
      author: bob.author,
      signing: true,
      actor: "02",
    })
    receiver = Automerge.change(receiver, d => {
      d.mine = "receiver draft"
    })
    const [mine] = Automerge.getHeads(receiver)

    const seen: Automerge.PatchSource[] = []
    receiver = Automerge.merge(
      Automerge.clone(receiver, {
        patchCallback: (_p, info) => seen.push(info.source),
      }),
      source,
    )
    assert.deepEqual(seen, ["merge"])
    assert.deepEqual(
      Automerge.getHeads(receiver).sort(),
      [mine, published].sort(),
    )
    assert.equal(receiver.counter, 1)
    assert.equal(receiver.draft, undefined)
    assert.equal(receiver.mine, "receiver draft")
    assert.equal(Automerge.hasHeads(receiver, [draft]), false)
    // Receiver's own draft is retained and still requested; the merged
    // history is not.
    assert.deepEqual(
      Automerge.requestSignatures(receiver).map(r => r.hash),
      [mine],
    )
    // The source remains a usable document with its draft.
    assert.deepEqual(Automerge.getHeads(source), [draft])
    source = Automerge.change(source, d => {
      d.draft = "still editable"
    })

    // Repeated merge deduplicates.
    receiver = Automerge.merge(receiver, source)
    assert.equal(Automerge.getAllChanges(receiver).length, 2)

    // Merged proof is re-exportable to an empty peer.
    const wire = concatBytes(
      Automerge.bundleFragmentMetadata(
        receiver,
        Automerge.getFragmentMetadata(receiver),
      ),
    )
    const empty = Automerge.load<CounterDoc>(wire, RECEIVER)
    assert.deepEqual(Automerge.getHeads(empty), [published])

    // Mode mismatch is rejected both ways and leaves both documents usable.
    let unsigned = Automerge.init<CounterDoc>({ actor: "09" })
    unsigned = Automerge.change(unsigned, d => {
      d.counter = 9
    })
    assert.throws(() => Automerge.merge(receiver, unsigned), RangeError)
    assert.throws(() => Automerge.merge(unsigned, receiver), RangeError)
    receiver = Automerge.change(receiver, d => {
      d.mine = "after mismatch"
    })
    unsigned = Automerge.change(unsigned, d => {
      d.counter = 10
    })
    assert.equal(receiver.mine, "after mismatch")
    assert.equal(unsigned.counter, 10)
  })

  it("saves only the published prefix, keeps drafts live, and emits them incrementally once published", async () => {
    const alice = await makeSigner()
    let doc = Automerge.init<CounterDoc>({
      author: alice.author,
      signing: true,
      actor: "01",
    })
    doc = Automerge.change(doc, d => {
      d.counter = 1
    })
    doc = await publishAll(doc, alice.sign)
    const [a] = Automerge.getHeads(doc)
    doc = Automerge.change(doc, d => {
      d.counter = 2
    })
    const [b] = Automerge.getHeads(doc)

    const snapshot = Automerge.save(doc)
    // Signed bytes are never read by an unsigned default load.
    assert.throws(() => Automerge.load(snapshot), RangeError)
    const reloaded = Automerge.load<CounterDoc>(snapshot, {
      signing: true,
      author: alice.author,
    })
    assert.deepEqual(Automerge.getHeads(reloaded), [a])
    assert.equal(reloaded.counter, 1)
    assert.equal(Automerge.hasUnpublishedChanges(reloaded), false)
    assert.deepEqual(Automerge.requestSignatures(reloaded), [])
    // The draft stays in the live document.
    assert.deepEqual(Automerge.getHeads(doc), [b])
    assert.equal(doc.counter, 2)
    assert.equal(Automerge.hasUnpublishedChanges(doc), true)

    assert.equal(Automerge.saveIncremental(doc).length, 0)
    doc = await publishAll(doc, alice.sign)
    const increment = Automerge.saveIncremental(doc)
    assert.ok(increment.length > 0)
    const caughtUp = Automerge.loadIncremental(reloaded, increment)
    assert.deepEqual(Automerge.getHeads(caughtUp), [b])
    assert.equal(caughtUp.counter, 2)

    // saveSince / full save of the published document round-trips.
    const full = Automerge.load<CounterDoc>(Automerge.save(doc), RECEIVER)
    assert.equal(full.counter, 2)
  })

  it("fixes the author at construction and guards new-author clones", async () => {
    const alice = await makeSigner()
    const bob = await makeSigner()

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
    assert.equal(
      (Automerge as unknown as { setAuthor?: unknown }).setAuthor,
      undefined,
    )

    let doc = Automerge.init<CounterDoc>({
      author: alice.author,
      signing: true,
    })
    doc = Automerge.change(doc, d => {
      d.title = "v1"
    })
    const [h1] = Automerge.getHeads(doc)

    // Signing mode is fixed; same-author clone is allowed; a different author
    // is rejected while a draft is unpublished, without invalidating the source.
    assert.throws(() => Automerge.clone(doc, { signing: false }), RangeError)
    const same = Automerge.clone(doc, { author: alice.author })
    assert.equal(Automerge.getAuthor(same), alice.author)
    assert.deepEqual(
      Automerge.requestSignatures(same).map(r => r.hash),
      [h1],
    )
    assert.throws(
      () => Automerge.clone(doc, { author: bob.author }),
      RangeError,
    )
    assert.throws(() => Automerge.clone(doc, { author: "010203" }), RangeError)
    doc = Automerge.change(doc, d => {
      d.title = "v2"
    })
    assert.equal(doc.title, "v2")

    // A signed historical view cannot be cloned (fork_at is unsupported),
    // with or without an author override, and the view stays readable.
    const viewAtV1 = Automerge.view(doc, [h1])
    assert.throws(() => Automerge.clone(viewAtV1), RangeError)
    assert.throws(
      () => Automerge.clone(viewAtV1, { author: alice.author }),
      RangeError,
    )
    assert.throws(
      () => Automerge.clone(viewAtV1, { author: bob.author }),
      RangeError,
    )
    assert.equal(viewAtV1.title, "v1")
    assert.equal(doc.title, "v2")

    // Once published, Bob may take a full fork with a fresh actor; the source
    // author and historical bindings are unchanged and no eligibility moves.
    doc = await publishAll(doc, alice.sign)
    const forked = Automerge.clone(doc, { author: bob.author })
    assert.equal(Automerge.getAuthor(forked), bob.author)
    assert.notEqual(Automerge.getActorId(forked), Automerge.getActorId(doc))
    assert.deepEqual(Automerge.requestSignatures(forked), [])
    assert.equal(
      Automerge.getAuthorForActor(forked, Automerge.getActorId(doc)),
      alice.author,
    )
    assert.equal(Automerge.getAuthor(doc), alice.author)
    const forkedEdit = Automerge.change(forked, d => {
      d.bob = "hi"
    })
    const [bobHead] = Automerge.getHeads(forkedEdit)
    const [request] = Automerge.requestSignatures(forkedEdit)
    assert.equal(request.hash, bobHead)
    assert.equal(request.author, bob.author)

    // Unsigned historical clones keep working, with or without an author.
    let unsigned = Automerge.init<CounterDoc>({ actor: "09" })
    unsigned = Automerge.change(unsigned, d => {
      d.counter = 1
    })
    const [u1] = Automerge.getHeads(unsigned)
    unsigned = Automerge.change(unsigned, d => {
      d.counter = 2
    })
    const historical = Automerge.view(unsigned, [u1])
    const plain = Automerge.clone(historical)
    assert.equal(plain.counter, 1)
    assert.equal(Automerge.getAuthor(plain), null)
    const authored = Automerge.clone(historical, { author: "abcd" })
    assert.equal(authored.counter, 1)
    assert.equal(Automerge.getAuthor(authored), "abcd")
    assert.deepEqual(Automerge.getHeads(authored), [u1])
    const edited = Automerge.change(authored, d => {
      d.counter = 3
    })
    assert.equal(
      Automerge.decodeChange(Automerge.getLastLocalChange(edited)!).author,
      "abcd",
    )
    // The source is untouched by any of this.
    assert.equal(unsigned.counter, 2)
    assert.equal(Automerge.getAuthor(unsigned), null)
    const sameAuthorView = Automerge.clone(Automerge.view(authored, [u1]), {
      author: "abcd",
    })
    assert.equal(sameAuthorView.counter, 1)
  })

  it("never lets a fixed author adopt an actor bound to another author, and anonymize yields an unsigned diagnostic copy", async () => {
    const alice = await makeSigner()
    const bob = await makeSigner()
    let doc = Automerge.init<CounterDoc>({
      author: alice.author,
      signing: true,
      actor: "01",
    })
    doc = Automerge.change(doc, d => {
      d.title = "a"
    })
    const [a] = Automerge.getHeads(doc)
    doc = await publishAll(doc, alice.sign)
    const bytes = Automerge.save(doc)

    // Bob cannot continue Alice's actor chain through any entry point.
    const bound = /belongs to a different author/
    assert.throws(
      () => Automerge.clone(doc, { author: bob.author, actor: "01" }),
      bound,
    )
    assert.throws(
      () =>
        Automerge.load<CounterDoc>(bytes, {
          signing: true,
          author: bob.author,
          actor: "01",
        }),
      bound,
    )
    const bobs = Automerge.clone(doc, { author: bob.author })
    assert.throws(() => Automerge.clone(bobs, "01"), bound)
    assert.throws(() => Automerge.clone(bobs, { actor: "01" }), bound)
    assert.throws(
      () => Automerge.clone(bobs, { author: bob.author, actor: "01" }),
      bound,
    )
    // The source is unaffected and remains Alice's.
    assert.equal(Automerge.getAuthor(doc), alice.author)
    assert.equal(Automerge.getActorId(doc), "01")
    // Same author or a fresh actor is fine, as are unsigned documents.
    assert.equal(
      Automerge.getActorId(
        Automerge.clone(doc, { author: alice.author, actor: "01" }),
      ),
      "01",
    )
    assert.equal(Automerge.getActorId(Automerge.clone(bobs, "02")), "02")
    let unsigned = Automerge.init<CounterDoc>({ author: "aa", actor: "01" })
    unsigned = Automerge.change(unsigned, d => {
      d.counter = 1
    })
    assert.equal(
      Automerge.getActorId(
        Automerge.clone(unsigned, { author: "bb", actor: "01" }),
      ),
      "01",
    )

    // Bob configured actor 01 before importing Alice's published chain. The
    // first local change fails before any operation is applied.
    let late = Automerge.init<CounterDoc>({
      author: bob.author,
      signing: true,
      actor: "01",
    })
    late = Automerge.loadIncremental(late, bytes)
    assert.deepEqual(Automerge.getHeads(late), [a])
    assert.throws(() =>
      Automerge.change(late, d => {
        d.title = "b"
      }),
    )
    assert.deepEqual(Automerge.getHeads(late), [a])
    assert.equal(Automerge.hasUnpublishedChanges(late), false)
    assert.deepEqual(Automerge.requestSignatures(late), [])

    // anonymize: new unsigned diagnostic history, no proof, cannot re-enter
    // required mode, source untouched (draft included).
    doc = Automerge.change(doc, d => {
      d.title = "draft"
    })
    const anon = Automerge.anonymize(doc)
    assert.equal(Automerge.getAllChanges(anon).length, 2)
    assert.equal(Automerge.hasUnpublishedChanges(anon), false)
    assert.throws(() => Automerge.requestSignatures(anon), RangeError)
    assert.equal(Automerge.getAuthor(anon), null)
    assert.ok(Automerge.load(Automerge.save(anon)))
    assert.throws(() =>
      Automerge.load(Automerge.save(anon), {
        signing: true,
        author: bob.author,
      }),
    )
    assert.throws(() => Automerge.merge(Automerge.clone(doc), anon), /mode/)
    assert.equal(doc.title, "draft")
    assert.equal(Automerge.hasUnpublishedChanges(doc), true)
    assert.equal(Automerge.requestSignatures(doc).length, 1)
  })

  it("preserves attached proofs through commit/fragment transport and rejects forged metadata", async () => {
    const alice = await makeSigner()
    let doc = Automerge.init<CounterDoc>({
      author: alice.author,
      signing: true,
      actor: "01",
    })
    doc = Automerge.change(doc, d => {
      d.title = "v1"
    })
    doc = Automerge.change(doc, d => {
      d.title = "v2"
    })
    doc = await publishAll(doc, alice.sign)
    const commits = Automerge.getCommits(doc)
    assert.equal(commits.length, 2)
    const rawChange = Automerge.getLastLocalChange(doc)!
    // Exported commit bytes carry the proof beyond the raw change.
    const last = commits.find(c => c.head === Automerge.getHeads(doc)[0])!
    assert.ok(last.bytes.length > rawChange.length)

    const receiver = Automerge.init<CounterDoc>(RECEIVER)
    // Raw change in place of the signed bytes, tampered bytes, wrong head,
    // wrong parents: all rejected without mutation.
    assert.throws(
      () => Automerge.addCommits(receiver, [{ ...last, bytes: rawChange }]),
      RangeError,
    )
    const tampered = new Uint8Array(last.bytes)
    tampered[tampered.length - 1] ^= 0xff
    assert.throws(
      () => Automerge.addCommits(receiver, [{ ...last, bytes: tampered }]),
      RangeError,
    )
    assert.throws(
      () =>
        Automerge.addCommits(receiver, [{ ...last, head: "ff".repeat(32) }]),
      RangeError,
    )
    assert.throws(
      () => Automerge.addCommits(receiver, [{ ...last, parents: [] }]),
      RangeError,
    )
    const [fragmentMeta] = Automerge.getFragmentMetadata(doc)
    const [fragment] = Automerge.getFragments(doc, { start: 0 })
    assert.throws(
      () =>
        Automerge.addFragments(receiver, [
          { ...fragment, members: [...fragment.members, "ff".repeat(32)] },
        ]),
      RangeError,
    )
    assert.throws(
      () =>
        Automerge.addFragments(receiver, [
          { ...fragment, level: fragment.level + 1 },
        ]),
      RangeError,
    )
    assert.deepEqual(Automerge.getHeads(receiver), [])
    assert.ok(fragmentMeta)

    // Raw inspectors remain available but are not authenticated transport.
    assert.throws(
      () => Automerge.applyChanges(receiver, Automerge.getAllChanges(doc)),
      RangeError,
    )
    assert.throws(
      () => Automerge.loadIncremental(receiver, rawChange),
      RangeError,
    )
    // Bundling non-canonical member sets yields no fake authenticated output.
    assert.deepEqual(
      Automerge.bundleFragmentMetadata(doc, [
        { ...fragmentMeta, members: [fragmentMeta.head] },
      ]).filter(bytes => fragmentMeta.members.length > 1 && bytes.length > 0),
      [],
    )

    const complete = Automerge.addCommits(receiver, commits)
    assert.deepEqual(Automerge.getHeads(complete), Automerge.getHeads(doc))
    assert.equal(complete.title, "v2")

    // Signed sync is an explicit error in both directions.
    const [, msg] = Automerge.generateSyncMessage(
      Automerge.change(Automerge.init<CounterDoc>(), d => {
        d.counter = 1
      }),
      Automerge.initSyncState(),
    )
    assert.ok(msg)
    assert.throws(
      () => Automerge.generateSyncMessage(complete, Automerge.initSyncState()),
      RangeError,
    )
    assert.throws(
      () =>
        Automerge.receiveSyncMessage(complete, Automerge.initSyncState(), msg),
      RangeError,
    )
    assert.equal(complete.title, "v2")
  })
})
