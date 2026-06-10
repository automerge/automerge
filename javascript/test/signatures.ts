import { default as assert } from "assert"
import * as Automerge from "../src/entrypoints/fullfat_node.js"

describe("signatures", () => {
  const author = "07".repeat(32)

  function signPending<T>(doc: Automerge.Doc<T>, byte: number): Automerge.Doc<T> {
    const signatures = Automerge.initSignatureState()
    ;[doc] = Automerge.reconcileSignatures(doc, signatures)
    const requests = signatures.pendingSigningRequests()
    assert.ok(requests.length > 0)
    for (const request of requests) {
      signatures.completeSigning(request.hash, new Uint8Array(64).fill(byte))
    }
    ;[doc] = Automerge.reconcileSignatures(doc, signatures)
    return doc
  }

  function loadSignedAcceptingAll<T>(saved: Uint8Array): Automerge.Doc<T> {
    let doc = Automerge.load<T>(saved, { signing: true })
    const verifier = Automerge.initSignatureState()
    while (true) {
      ;[doc] = Automerge.reconcileSignatures(doc, verifier)
      const requests = verifier.pendingVerificationRequests()
      if (requests.length === 0) break
      for (const request of requests) {
        verifier.completeVerification(request.id, true)
      }
    }
    ;[doc] = Automerge.reconcileSignatures(doc, verifier)
    return doc
  }

  it("keeps signed changes invisible until verification completes", () => {
    let source = Automerge.init<any>({ author })
    source = Automerge.change(source, doc => {
      doc.key = "value"
    })
    const change = Automerge.getAllChanges(source)[0]

    let remote = Automerge.init<any>({ signing: true })
    ;[remote] = Automerge.applyChanges(remote, [change])
    assert.equal(remote.key, undefined)

    const signatureState = Automerge.initSignatureState()
    let report
    ;[remote, report] = Automerge.reconcileSignatures(remote, signatureState)
    assert.equal(report.verificationRequested, 1)
    assert.equal(remote.key, undefined)

    const requests = signatureState.pendingVerificationRequests()
    assert.equal(requests.length, 1)
    assert.equal(requests[0].hash, Automerge.getHeads(source)[0])
    assert.equal(requests[0].author, author)
    assert.equal(requests[0].signature, undefined)

    signatureState.completeVerification(requests[0].id, true)
    ;[remote, report] = Automerge.reconcileSignatures(remote, signatureState)
    assert.equal(report.verificationAccepted, 1)
    assert.equal(remote.key, "value")
  })

  it("verified same-author child verifies ancestors", () => {
    let source = Automerge.init<any>({ author })
    source = Automerge.change(source, doc => {
      doc.first = "one"
    })
    source = Automerge.change(source, doc => {
      doc.second = "two"
    })
    const changes = Automerge.getAllChanges(source)

    let remote = Automerge.init<any>({ signing: true })
    ;[remote] = Automerge.applyChanges(remote, changes)
    const signatureState = Automerge.initSignatureState()
    let report
    ;[remote, report] = Automerge.reconcileSignatures(remote, signatureState)
    assert.equal(report.verificationRequested, 1)

    const request = signatureState.pendingVerificationRequests()[0]
    assert.equal(request.hash, Automerge.getHeads(source)[0])
    signatureState.completeVerification(request.id, true)
    ;[remote, report] = Automerge.reconcileSignatures(remote, signatureState)

    assert.equal(report.verificationAccepted, 1)
    assert.equal(remote.first, "one")
    assert.equal(remote.second, "two")
  })

  it("keeps rejected signed changes invisible", () => {
    let source = Automerge.init<any>({ author })
    source = Automerge.change(source, doc => {
      doc.key = "value"
    })
    const change = Automerge.getAllChanges(source)[0]

    let remote = Automerge.init<any>({ signing: true })
    const signatureState = Automerge.initSignatureState()
    let report
    ;[remote] = Automerge.applyChanges(remote, [change])
    ;[remote] = Automerge.reconcileSignatures(remote, signatureState)
    const request = signatureState.pendingVerificationRequests()[0]
    signatureState.completeVerification(request.id, false)
    ;[remote, report] = Automerge.reconcileSignatures(remote, signatureState)

    assert.equal(report.verificationRejected, 1)
    assert.equal(remote.key, undefined)
  })

  it("normal getChanges omits changes awaiting signatures", () => {
    let doc = Automerge.init<any>({ author, signing: true })
    doc = Automerge.change(doc, d => {
      d.key = "value"
    })
    assert.equal(Automerge.getAllChanges(doc).length, 0)

    const signatures = Automerge.initSignatureState()
    ;[doc] = Automerge.reconcileSignatures(doc, signatures)
    signatures.completeSigning(Automerge.getHeads(doc)[0], new Uint8Array(64).fill(7))
    ;[doc] = Automerge.reconcileSignatures(doc, signatures)
    const changes = Automerge.getAllChanges(doc)
    assert.equal(changes.length, 1)
  })

  it("normal save filters signing-incomplete local changes", () => {
    let doc = Automerge.init<any>({ author, signing: true })
    doc = Automerge.change(doc, d => {
      d.key = "value"
    })
    let loaded = Automerge.load<any>(Automerge.save(doc), { signing: true })
    assert.equal(loaded.key, undefined)

    doc = signPending(doc, 8)
    loaded = loadSignedAcceptingAll<any>(Automerge.save(doc))
    assert.equal(loaded.key, "value")
  })

  it("normal save filters unsigned list suffix", () => {
    let doc = Automerge.init<any>({ author, signing: true })
    doc = Automerge.change(doc, d => {
      d.list = ["one", "two"]
    })
    doc = signPending(doc, 13)

    doc = Automerge.change(doc, d => {
      d.list.push("three")
    })
    const loaded = loadSignedAcceptingAll<any>(Automerge.save(doc))
    assert.deepEqual(loaded.list, ["one", "two"])
  })

  it("normal save filters unsigned text suffix", () => {
    let doc = Automerge.init<any>({ author, signing: true })
    doc = Automerge.change(doc, d => {
      d.text = "hello"
    })
    doc = signPending(doc, 14)

    doc = Automerge.change(doc, d => {
      Automerge.splice(d, ["text"], 5, 0, " world")
    })
    const loaded = loadSignedAcceptingAll<any>(Automerge.save(doc))
    assert.equal(loaded.text, "hello")
  })

  it("normal save filters unsigned mark", () => {
    let doc = Automerge.init<any>({ author, signing: true })
    doc = Automerge.change(doc, d => {
      d.text = "hello"
    })
    doc = signPending(doc, 15)

    doc = Automerge.change(doc, d => {
      Automerge.mark(
        d,
        ["text"],
        { start: 0, end: 5, expand: "both" },
        "bold",
        true,
      )
    })
    const loaded = loadSignedAcceptingAll<any>(Automerge.save(doc))
    assert.equal(loaded.text, "hello")
    assert.deepEqual(Automerge.marks(loaded, ["text"]), [])
  })

  it("normal save filters unsigned unmark", () => {
    let doc = Automerge.init<any>({ author, signing: true })
    doc = Automerge.change(doc, d => {
      d.text = "hello"
    })
    doc = signPending(doc, 16)
    doc = Automerge.change(doc, d => {
      Automerge.mark(
        d,
        ["text"],
        { start: 0, end: 5, expand: "both" },
        "bold",
        true,
      )
    })
    doc = signPending(doc, 17)

    doc = Automerge.change(doc, d => {
      Automerge.unmark(d, ["text"], { start: 0, end: 5 }, "bold")
    })
    const loaded = loadSignedAcceptingAll<any>(Automerge.save(doc))
    assert.deepEqual(Automerge.marks(loaded, ["text"]), [
      { name: "bold", value: true, start: 0, end: 5 },
    ])
  })

  it("keeps signed loadIncremental invisible until retained signatures verify", () => {
    let source = Automerge.init<any>({ author, signing: true })
    source = Automerge.change(source, doc => {
      doc.first = "one"
    })
    source = Automerge.change(source, doc => {
      doc.second = "two"
    })
    const head = Automerge.getHeads(source)[0]

    const signing = Automerge.initSignatureState()
    let report
    ;[source, report] = Automerge.reconcileSignatures(source, signing)
    assert.equal(report.signingRequested, 1)
    signing.completeSigning(head, new Uint8Array(64).fill(12))
    ;[source] = Automerge.reconcileSignatures(source, signing)

    const saved = Automerge.save(source)
    let loaded = Automerge.init<any>({ signing: true })
    loaded = Automerge.loadIncremental(loaded, saved)
    assert.equal(loaded.first, undefined)
    assert.equal(loaded.second, undefined)

    const verifier = Automerge.initSignatureState()
    ;[loaded, report] = Automerge.reconcileSignatures(loaded, verifier)
    assert.equal(report.verificationRequested, 1)
    const request = verifier.pendingVerificationRequests()[0]
    assert.equal(request.hash, head)
    assert.deepEqual(request.signature, new Uint8Array(64).fill(12))

    verifier.completeVerification(request.id, true)
    ;[loaded, report] = Automerge.reconcileSignatures(loaded, verifier)
    assert.equal(report.verificationAccepted, 1)
    assert.equal(loaded.first, "one")
    assert.equal(loaded.second, "two")
  })

  it("keeps signed loads invisible until retained signatures verify", () => {
    let source = Automerge.init<any>({ author, signing: true })
    source = Automerge.change(source, doc => {
      doc.first = "one"
    })
    source = Automerge.change(source, doc => {
      doc.second = "two"
    })
    const head = Automerge.getHeads(source)[0]

    const signing = Automerge.initSignatureState()
    let report
    ;[source, report] = Automerge.reconcileSignatures(source, signing)
    assert.equal(report.signingRequested, 1)
    signing.completeSigning(head, new Uint8Array(64).fill(11))
    ;[source] = Automerge.reconcileSignatures(source, signing)

    const saved = Automerge.save(source)
    let loaded = Automerge.load<any>(saved, { signing: true })
    assert.equal(loaded.first, undefined)
    assert.equal(loaded.second, undefined)

    const verifier = Automerge.initSignatureState()
    ;[loaded, report] = Automerge.reconcileSignatures(loaded, verifier)
    assert.equal(report.verificationRequested, 1)
    const request = verifier.pendingVerificationRequests()[0]
    assert.equal(request.hash, head)
    assert.deepEqual(request.signature, new Uint8Array(64).fill(11))

    verifier.completeVerification(request.id, true)
    ;[loaded, report] = Automerge.reconcileSignatures(loaded, verifier)
    assert.equal(report.verificationAccepted, 1)
    assert.equal(loaded.first, "one")
    assert.equal(loaded.second, "two")
  })
})
