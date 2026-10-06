import { default as assert } from "assert"
import * as Automerge from "../src/entrypoints/fullfat_node.js"

/** A fresh Ed25519 key pair, with the public half as an author string. */
async function ed25519Keys(): Promise<{ keys: CryptoKeyPair; author: string }> {
  const keys = (await crypto.subtle.generateKey("Ed25519", true, [
    "sign",
    "verify",
  ])) as CryptoKeyPair
  const publicBytes = new Uint8Array(
    await crypto.subtle.exportKey("raw", keys.publicKey),
  )
  const author = Array.from(publicBytes, byte =>
    byte.toString(16).padStart(2, "0"),
  ).join("")
  return { keys, author }
}

/** Sign every outstanding request with `privateKey`. */
async function signRequests(
  doc: Automerge.Doc<unknown>,
  privateKey: CryptoKey,
): Promise<Automerge.SignatureInput[]> {
  return Promise.all(
    Automerge.signingRequests(doc).map(async request => ({
      hash: request.hash,
      scheme: "ed25519" as const,
      signature: new Uint8Array(
        await crypto.subtle.sign(
          "Ed25519",
          privateKey,
          Uint8Array.from(request.bytesToSign).buffer,
        ),
      ),
    })),
  )
}

describe("detached signatures", () => {
  it("rejects invalid signature modes instead of falling back to legacy", () => {
    assert.throws(() =>
      Automerge.init<any>({ signatures: "typo" as "authenticated" }),
    )
    assert.throws(() =>
      Automerge.init<any>({ signatures: null as unknown as "authenticated" }),
    )
    // The mode is fixed when a document is created, so a clone inherits it
    // rather than switching.
    const legacy = Automerge.init<any>()
    assert.throws(() =>
      Automerge.clone(legacy, { signatures: "authenticated" }),
    )
  })

  it("requests the actor maxima and names the exact bytes to sign", async () => {
    const { keys, author } = await ed25519Keys()
    let doc = Automerge.init<any>({ author, signatures: "authenticated" })
    doc = Automerge.change(doc, d => {
      d.value = "signed"
    })

    const requests = Automerge.signingRequests(doc)
    assert.equal(requests.length, 1)
    assert.equal(requests[0].author, author)
    assert.deepEqual(
      Array.from(requests[0].bytesToSign),
      Array.from(Buffer.from(requests[0].hash, "hex")),
    )

    // Nothing is exportable until the maximum carries a record which verifies.
    assert.equal(Automerge.getCommits(doc).length, 0)

    // A record under the wrong key, and a custom record, are both retained and
    // relayed, and neither authenticates anything.
    const other = await ed25519Keys()
    doc = Automerge.addSignatures(doc, [
      ...(await signRequests(doc, other.keys.privateKey)),
      {
        hash: requests[0].hash,
        scheme: "custom",
        signature: new Uint8Array([1, 2, 3]),
      },
    ])
    assert.equal(Automerge.signingRequests(doc).length, 1)
    assert.equal(Automerge.getCommits(doc).length, 0)

    // The record under its own key is what settles it.
    doc = Automerge.addSignatures(doc, await signRequests(doc, keys.privateKey))
    assert.equal(Automerge.signingRequests(doc).length, 0)
    const commits = Automerge.getCommits(doc)
    assert.equal(commits.length, 1)

    // All three records travelled with it, so a receiver can relay what it
    // cannot interpret.
    let target = Automerge.init<any>({ signatures: "authenticated" })
    target = Automerge.addCommits(target, commits)
    assert.equal(target.value, "signed")

    // A further record on a settled change is retained like any other and
    // settles nothing further: only one verifying under the change's own author
    // ever does.
    doc = Automerge.addSignatures(doc, [
      {
        hash: requests[0].hash,
        scheme: "custom",
        signature: new Uint8Array([9]),
      },
    ])
    assert.equal(Automerge.signingRequests(doc).length, 0)
    assert.equal(Automerge.getCommits(doc).length, 1)
  })

  it("completes an Ed25519 fragment workflow through an opaque relay", async () => {
    const { keys, author } = await ed25519Keys()
    let source = Automerge.init<any>({ author, signatures: "authenticated" })
    for (const value of ["s", "si", "sig"]) {
      source = Automerge.change(source, doc => {
        doc.value = value
      })
    }
    source = Automerge.addSignatures(
      source,
      await signRequests(source, keys.privateKey),
    )

    // Simulate an opaque storage relay by copying only transport byte fields.
    const commits = Automerge.getCommits(source).map(commit => ({
      ...commit,
      bytes: commit.bytes.slice(),
    }))
    const fragments = Automerge.getFragments(source).map(fragment => ({
      ...fragment,
      bytes: fragment.bytes.slice(),
    }))
    assert.ok(commits.length + fragments.length > 0)

    // Verification happens inside Automerge, so import is all it takes.
    let target = Automerge.init<any>({ signatures: "authenticated" })
    target = Automerge.addCommits(target, commits)
    target = Automerge.addFragments(target, fragments)
    assert.equal(target.value, "sig")
  })

  it("refuses history whose records were stripped", async () => {
    const { keys, author } = await ed25519Keys()
    let source = Automerge.init<any>({ author })
    source = Automerge.change(source, doc => {
      doc.value = "remote"
    })
    source = Automerge.addSignatures(
      source,
      await signRequests(source, keys.privateKey),
    )
    const commits = Automerge.getCommits(source)

    // A peer which drops the trailing signature chunk is refused rather than
    // silently withheld, so stripping records is a visible failure.
    const target = Automerge.init<any>({ signatures: "authenticated" })
    const stripped = commits.map(commit => ({
      ...commit,
      bytes: commit.bytes.slice(
        0,
        Automerge.getLastLocalChange(source)!.length,
      ),
    }))
    assert.throws(() => Automerge.addCommits(target, stripped))
    assert.equal(target.value, undefined)

    assert.equal(Automerge.addCommits(target, commits).value, "remote")
  })

  it("requires a document chunk to be covered, unless it is our own storage", async () => {
    const { keys, author } = await ed25519Keys()
    let source = Automerge.init<any>({ author, signatures: "authenticated" })
    source = Automerge.change(source, doc => {
      doc.value = "snapshot"
    })

    // A document carries its own authors, so whether it is covered is decidable
    // on arrival: unsigned, it is refused rather than held.
    const unsigned = Automerge.save(source)
    assert.throws(() =>
      Automerge.load<any>(unsigned, { signatures: "authenticated" }),
    )

    // The application's own storage is asserted explicitly, which is the only
    // way an unsigned local draft survives a restart.
    const trusted = Automerge.load<any>(unsigned, {
      signatures: "authenticated",
      ownStorage: true,
      author,
    })
    assert.equal(trusted.value, "snapshot")
    assert.equal(Automerge.signingRequests(trusted).length, 1)

    // Signed, the records in the same stream cover it before the load returns.
    source = Automerge.addSignatures(
      source,
      await signRequests(source, keys.privateKey),
    )
    const covered = Automerge.load<any>(Automerge.save(source), {
      signatures: "authenticated",
    })
    assert.equal(covered.value, "snapshot")
  })

  it("keeps local commits visible and emits patches when history is admitted", async () => {
    const { keys, author } = await ed25519Keys()
    let local = Automerge.init<any>({ author, signatures: "authenticated" })
    local = Automerge.change(local, d => {
      d.mine = "draft"
    })
    // A local commit is visible immediately, since signing it may need a key
    // held outside Automerge.
    assert.equal(local.mine, "draft")

    const remote = await ed25519Keys()
    let peer = Automerge.init<any>({ author: remote.author })
    peer = Automerge.change(peer, d => {
      d.theirs = "sent"
    })
    peer = Automerge.addSignatures(
      peer,
      await signRequests(peer, remote.keys.privateKey),
    )

    let patches: Automerge.Patch[] = []
    local = Automerge.addCommits(local, Automerge.getCommits(peer), {
      patchCallback: p => patches.push(...p),
    })
    assert.equal(local.theirs, "sent")
    assert.ok(patches.some(patch => patch.path[0] === "theirs"))
    assert.equal(local.mine, "draft")
  })
})
