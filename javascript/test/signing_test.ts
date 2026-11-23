import { default as assert } from "assert"
import { webcrypto } from "node:crypto"
import * as Automerge from "../src/entrypoints/fullfat_node.js"

const subtle = webcrypto.subtle

type CounterDoc = {
  counter?: number
  title?: string
  bob?: string
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
  const requests = Automerge.signingRequests(doc)
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

/** The published history of `doc` as a single loadable byte string. */
function wireOf<T>(doc: Automerge.Doc<T>): Uint8Array {
  return concatBytes(
    Automerge.bundleFragmentMetadata(doc, Automerge.getFragmentMetadata(doc)),
  )
}

/** One signing document with a single unsigned draft. */
async function draftDoc() {
  const alice = await makeSigner()
  let doc = Automerge.init<CounterDoc>({
    author: alice.author,
    signing: true,
    actor: "01",
  })
  doc = Automerge.change(doc, d => {
    d.title = "v1"
  })
  return { alice, doc }
}

/** `draftDoc` plus the signature for its one outstanding request. */
async function draftWithInputs() {
  const { alice, doc } = await draftDoc()
  const requests = Automerge.signingRequests(doc)
  return {
    alice,
    doc,
    requests,
    inputs: await signRequests(requests, alice.sign),
  }
}

/**
 * A signing document whose own draft `a` is unpublished and which already
 * holds a verified peer commit depending on `a`. Publishing `a` applies that
 * peer commit, which is what makes `insertSignatures` emit patches.
 */
async function pendingDependent() {
  const alice = await makeSigner()
  const bob = await makeSigner()
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

  let bobDoc = Automerge.load<CounterDoc>(wireOf(aliceDoc), {
    signing: true,
    author: bob.author,
    actor: "02",
  })
  bobDoc = Automerge.change(bobDoc, d => {
    d.bob = "dependent"
  })
  bobDoc = await publishAll(bobDoc, bob.sign)
  const [b] = Automerge.getHeads(bobDoc)
  const bobCommit = Automerge.getCommits(bobDoc).find(c => c.head === b)!

  const doc = Automerge.addCommits(laggard, [bobCommit])
  assert.deepEqual(Automerge.getHeads(doc), [a])
  const inputs = await signRequests(Automerge.signingRequests(doc), alice.sign)
  return { doc, inputs }
}

describe("fragment signing API", () => {
  describe("options", () => {
    it("fixes the author when init is given signing and an author", async () => {
      const alice = await makeSigner()
      const doc = Automerge.init<CounterDoc>({
        author: alice.author,
        signing: true,
      })
      assert.equal(Automerge.getAuthor(doc), alice.author)
    })

    it("rejects init in signing mode without an author", () => {
      assert.throws(
        () => Automerge.init<CounterDoc>({ signing: true }),
        RangeError,
      )
    })

    it("rejects init in signing mode with a non-key author", () => {
      assert.throws(
        () => Automerge.init<CounterDoc>({ signing: true, author: "010203" }),
        RangeError,
      )
    })

    it("rejects load in signing mode without an author", () => {
      assert.throws(
        () => Automerge.load<CounterDoc>(new Uint8Array(0), { signing: true }),
        RangeError,
      )
    })

    it("does not export setAuthor", () => {
      assert.equal(
        (Automerge as unknown as { setAuthor?: unknown }).setAuthor,
        undefined,
      )
    })
  })

  describe("signingRequests", () => {
    it("returns one request per unsigned local commit", async () => {
      const alice = await makeSigner()
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
      assert.equal(Automerge.signingRequests(doc).length, 2)
    })

    it("yields the signing prefix followed by the raw change hash", async () => {
      const { doc } = await draftDoc()
      const [request] = Automerge.signingRequests(doc)
      assert.equal(request.hash, Automerge.getHeads(doc)[0])
      assert.deepEqual(request.message, messageFor(request.hash))
    })

    it("returns nothing once every local commit is published", async () => {
      const { alice, doc } = await draftDoc()
      const published = await publishAll(doc, alice.sign)
      assert.deepEqual(Automerge.signingRequests(published), [])
    })

    it("throws for an unsigned document", () => {
      const doc = Automerge.change(Automerge.init<CounterDoc>(), d => {
        d.counter = 1
      })
      assert.throws(() => Automerge.signingRequests(doc), RangeError)
    })

    it("is exposed on the next namespace", async () => {
      const { doc } = await draftDoc()
      assert.deepEqual(
        Automerge.next.signingRequests(doc),
        Automerge.signingRequests(doc),
      )
    })
  })

  describe("insertSignatures handoff", () => {
    it("supersedes the input document on completion", async () => {
      const { doc, inputs } = await draftWithInputs()
      const { doc: published } = Automerge.insertSignatures(doc, inputs)
      assert.notStrictEqual(published, doc)
      assert.throws(() => Automerge.change(doc, () => {}), RangeError)
      assert.equal(published.title, "v1")
    })

    it("returns the very same document object on rejection", async () => {
      const { doc } = await draftDoc()
      const result = Automerge.insertSignatures(doc, [
        7,
      ] as unknown as Automerge.SignatureInput[])
      assert.equal(result.outcome.status, "rejected")
      assert.strictEqual(result.doc, doc)
    })

    it("leaves the input document writable after rejection", async () => {
      const { doc } = await draftDoc()
      Automerge.insertSignatures(doc, [
        7,
      ] as unknown as Automerge.SignatureInput[])
      const next = Automerge.change(doc, d => {
        d.title = "v2"
      })
      assert.equal(next.title, "v2")
      assert.equal(Automerge.signingRequests(next).length, 2)
    })

    it("rejects a batch submitted to an unsigned document", async () => {
      const { inputs } = await draftWithInputs()
      let unsigned = Automerge.init<CounterDoc>({ actor: "09" })
      unsigned = Automerge.change(unsigned, d => {
        d.counter = 42
      })
      const result = Automerge.insertSignatures(unsigned, inputs)
      assert.equal(result.outcome.status, "rejected")
      assert.strictEqual(result.doc, unsigned)
    })

    // The `publicationFailed` arm needs two equivocating pending groups to
    // trigger; that scenario is pinned in Rust by
    // `signing::tests::batch::simultaneously_admissible_pending_equivocation_fails_publication_but_retains_state`.
  })

  describe("insertSignatures guards", () => {
    it("throws for an outdated document", async () => {
      const { doc, inputs } = await draftWithInputs()
      Automerge.change(doc, d => {
        d.title = "v2"
      })
      assert.throws(() => Automerge.insertSignatures(doc, inputs), RangeError)
    })

    it("throws for a historical view", async () => {
      const { doc, inputs } = await draftWithInputs()
      const [h1] = Automerge.getHeads(doc)
      const next = Automerge.change(doc, d => {
        d.title = "v2"
      })
      const historical = Automerge.view(next, [h1])
      assert.throws(
        () => Automerge.insertSignatures(historical, inputs),
        RangeError,
      )
    })

    it("throws when nested inside a change callback", async () => {
      const { doc, inputs } = await draftWithInputs()
      Automerge.change(doc, d => {
        assert.throws(
          () =>
            Automerge.insertSignatures(d as Automerge.Doc<CounterDoc>, inputs),
          RangeError,
        )
      })
    })

    it("throws for the outer document while its change callback is active", async () => {
      const { doc, inputs } = await draftWithInputs()
      const [input] = inputs
      Automerge.change(doc, () => {
        assert.throws(() => Automerge.insertSignatures(doc, input), RangeError)
      })
    })

    it("rejects merge in either direction while a change callback is active", async () => {
      const { doc } = await draftDoc()
      const target = Automerge.clone(doc)
      Automerge.change(doc, () => {
        assert.throws(() => Automerge.merge(target, doc), RangeError)
        assert.throws(() => Automerge.merge(doc, target), RangeError)
      })
    })

    it("leaves the document usable after a callback that threw", async () => {
      const { doc, inputs } = await draftWithInputs()
      const [input] = inputs
      const target = Automerge.clone(doc)
      const aborted = new Error("abort callback")
      assert.throws(
        () =>
          Automerge.change(doc, proxy => {
            proxy.title = "rolled back"
            assert.throws(() => Automerge.merge(target, doc), RangeError)
            throw aborted
          }),
        error => error === aborted,
      )
      assert.equal(doc.title, "v1")
      const result = Automerge.insertSignatures(doc, input)
      assert.equal(result.outcome.status, "completed")
      assert.equal(Automerge.hasUnpublishedChanges(result.doc), false)
    })
  })

  describe("insertSignatures outcome shape", () => {
    it("echoes hash, message and signature with a stored result", async () => {
      const { doc, requests, inputs } = await draftWithInputs()
      const [request] = requests
      const [input] = inputs
      const { outcome } = Automerge.insertSignatures(doc, [input])
      const [insertion] = completed(outcome)
      assert.equal(insertion.hash, request.hash)
      assert.deepEqual(insertion.message, input.message)
      assert.deepEqual(insertion.signature, input.signature)
      assert.deepEqual(insertion.result, { ok: true, insert: "stored" })
    })

    it("reports a repeated entry as a duplicate", async () => {
      const { doc, inputs } = await draftWithInputs()
      const [input] = inputs
      const { outcome } = Automerge.insertSignatures(doc, [input, input])
      assert.deepEqual(
        completed(outcome).map(i => i.result),
        [
          { ok: true, insert: "stored" },
          { ok: true, insert: "duplicate" },
        ],
      )
    })

    it("fails an unverifiable entry without failing the batch", async () => {
      const { doc, requests, inputs } = await draftWithInputs()
      const mallory = await makeSigner()
      const [request] = requests
      const { outcome } = Automerge.insertSignatures(doc, [
        {
          message: request.message,
          signature: await mallory.sign(request.message),
        },
        ...inputs,
      ])
      const insertions = completed(outcome)
      assert.equal(insertions[0].result.ok, false)
      assert.match(
        (insertions[0].result as { error: string }).error,
        /verification/,
      )
      assert.deepEqual(insertions[1].result, { ok: true, insert: "stored" })
    })

    it("fails an entry naming an unknown change", async () => {
      const { alice, doc } = await draftDoc()
      const unknown = messageFor("ee".repeat(32))
      const { outcome } = Automerge.insertSignatures(doc, [
        { message: unknown, signature: await alice.sign(unknown) },
      ])
      const [insertion] = completed(outcome)
      assert.equal(insertion.hash, "ee".repeat(32))
      assert.equal(insertion.result.ok, false)
    })

    it("names the offending index when an entry is malformed", async () => {
      const { doc, inputs } = await draftWithInputs()
      const [good] = inputs
      const { outcome } = Automerge.insertSignatures(doc, [
        good,
        { message: "text", signature: good.signature },
      ] as unknown as Automerge.SignatureInput[])
      assert.equal(outcome.status, "rejected")
      if (outcome.status !== "rejected") throw new Error("unreachable")
      assert.match(outcome.reason, /index 1/)
    })

    it("accepts a single input as a one-entry batch", async () => {
      const { doc, inputs } = await draftWithInputs()
      const [input] = inputs
      const target = Automerge.clone(doc)
      const single = Automerge.insertSignatures(doc, input)
      const batch = Automerge.insertSignatures(target, [input])
      assert.deepEqual(single.outcome, batch.outcome)
      assert.equal(completed(single.outcome).length, 1)
      assert.equal(Automerge.hasUnpublishedChanges(single.doc), false)
    })

    it("reports a malformed single input at index 0", async () => {
      const { doc } = await draftDoc()
      const { outcome } = Automerge.insertSignatures(
        doc,
        "not an input" as unknown as Automerge.SignatureInput,
      )
      assert.equal(outcome.status, "rejected")
      if (outcome.status !== "rejected") throw new Error("unreachable")
      assert.match(outcome.reason, /index 0/)
    })

    it("completes an empty batch with no insertions", async () => {
      const { doc } = await draftDoc()
      const { outcome } = Automerge.insertSignatures(doc, [])
      assert.deepEqual(outcome, { status: "completed", insertions: [] })
    })
  })

  describe("patch callbacks", () => {
    it("labels completion patches with the insertSignatures source", async () => {
      const { doc, inputs } = await pendingDependent()
      const infos: Automerge.PatchInfo<CounterDoc>[] = []
      const { doc: next, outcome } = Automerge.insertSignatures(doc, inputs, {
        patchCallback: (patches, info) => {
          assert.ok(patches.length > 0)
          infos.push(info)
        },
      })
      completed(outcome)
      assert.deepEqual(
        infos.map(i => i.source),
        ["insertSignatures"],
      )
      assert.equal(infos[0].after.bob, "dependent")
      assert.equal(next.bob, "dependent")
    })

    it("falls back to the document's own patch callback", async () => {
      const { doc, inputs } = await pendingDependent()
      const sources: Automerge.PatchSource[] = []
      const watched = Automerge.clone(doc, {
        patchCallback: (_p, info) => sources.push(info.source),
      })
      const { outcome } = Automerge.insertSignatures(watched, inputs)
      completed(outcome)
      assert.deepEqual(sources, ["insertSignatures"])
    })

    it("emits no patches when publication applies nothing", async () => {
      const { alice, doc } = await draftDoc()
      const sources: Automerge.PatchSource[] = []
      await publishAll(doc, alice.sign, {
        patchCallback: (_p, info) => sources.push(info.source),
      })
      assert.deepEqual(sources, [])
    })
  })

  describe("clone and author binding", () => {
    it("rejects a clone that restates the signing mode", async () => {
      const { doc } = await draftDoc()
      assert.throws(() => Automerge.clone(doc, { signing: false }), RangeError)
    })

    it("allows a clone keeping the same author", async () => {
      const { alice, doc } = await draftDoc()
      const same = Automerge.clone(doc, { author: alice.author })
      assert.equal(Automerge.getAuthor(same), alice.author)
      assert.deepEqual(
        Automerge.signingRequests(same).map(r => r.hash),
        Automerge.getHeads(doc),
      )
    })

    it("rejects a clone for a different author while a draft is unpublished", async () => {
      const { doc } = await draftDoc()
      const bob = await makeSigner()
      assert.throws(
        () => Automerge.clone(doc, { author: bob.author }),
        RangeError,
      )
      assert.equal(doc.title, "v1")
    })

    it("rejects adopting an actor bound to another author", async () => {
      const { alice, doc } = await draftDoc()
      const bob = await makeSigner()
      const published = await publishAll(doc, alice.sign)
      assert.throws(
        () => Automerge.clone(published, { author: bob.author, actor: "01" }),
        /associated with a different author identity/,
      )
      assert.equal(Automerge.getAuthor(published), alice.author)
    })
  })

  describe("hasUnpublishedChanges", () => {
    it("is false for an unsigned document", () => {
      const doc = Automerge.change(Automerge.init<CounterDoc>(), d => {
        d.counter = 42
      })
      assert.equal(Automerge.hasUnpublishedChanges(doc), false)
    })

    it("does not itself publish or commit anything", async () => {
      const { doc } = await draftDoc()
      assert.equal(Automerge.hasUnpublishedChanges(doc), true)
      assert.equal(Automerge.hasUnpublishedChanges(doc), true)
      assert.deepEqual(
        Automerge.signingRequests(doc).map(r => r.hash),
        Automerge.getHeads(doc),
      )
    })
  })

  describe("merge", () => {
    it("progresses the local document with the remote's published history", async () => {
      const { alice, doc } = await draftDoc()
      const source = await publishAll(doc, alice.sign)
      const bob = await makeSigner()
      const sources: Automerge.PatchSource[] = []
      const receiver = Automerge.merge(
        Automerge.init<CounterDoc>({
          author: bob.author,
          signing: true,
          patchCallback: (_p, info) => sources.push(info.source),
        }),
        source,
      )
      assert.deepEqual(sources, ["merge"])
      assert.deepEqual(Automerge.getHeads(receiver), Automerge.getHeads(source))
      assert.equal(receiver.title, "v1")
    })
  })

  describe("end to end", () => {
    it("publishes local changes and replicates them to a receiver", async () => {
      const alice = await makeSigner()
      let doc = Automerge.init<CounterDoc>({
        author: alice.author,
        signing: true,
        actor: "01",
      })
      for (let i = 1; i <= 3; i++) {
        doc = Automerge.change(doc, { time: i }, d => {
          d.counter = i
        })
      }
      // Nothing is exportable until the local author has signed.
      assert.equal(Automerge.getFragmentMetadata(doc).length, 0)
      assert.equal(Automerge.hasUnpublishedChanges(doc), true)

      const requests = Automerge.signingRequests(doc)
      assert.equal(requests.length, 3)
      doc = await publishAll(doc, alice.sign)
      assert.equal(Automerge.hasUnpublishedChanges(doc), false)
      assert.equal(doc.counter, 3)

      const fragments = Automerge.getFragments(doc, { start: 0 })
      const commits = Automerge.getCommits(doc)
      assert.ok(fragments.length > 0)
      assert.ok(commits.length > 0)

      const fragmentSources: Automerge.PatchSource[] = []
      const viaFragments = Automerge.addFragments(
        Automerge.init<CounterDoc>(RECEIVER),
        fragments,
        { patchCallback: (_p, info) => fragmentSources.push(info.source) },
      )
      assert.deepEqual(fragmentSources, ["addFragments"])
      assert.deepEqual(
        Automerge.getHeads(viaFragments),
        Automerge.getHeads(doc),
      )
      assert.equal(viaFragments.counter, 3)
      // Imported history is never signing work for the receiver.
      assert.deepEqual(Automerge.signingRequests(viaFragments), [])
      assert.equal(Automerge.hasUnpublishedChanges(viaFragments), false)

      const commitSources: Automerge.PatchSource[] = []
      const viaCommits = Automerge.addCommits(
        Automerge.init<CounterDoc>(RECEIVER),
        commits,
        { patchCallback: (_p, info) => commitSources.push(info.source) },
      )
      assert.deepEqual(commitSources, ["addCommits"])
      assert.equal(viaCommits.counter, 3)

      const loaded = Automerge.load<CounterDoc>(wireOf(doc), RECEIVER)
      assert.deepEqual(Automerge.getHeads(loaded), Automerge.getHeads(doc))
      assert.equal(loaded.counter, 3)
    })
  })
})
