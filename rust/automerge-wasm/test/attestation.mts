import { describe, it, before } from "mocha";
import assert from "assert";
import { webcrypto } from "node:crypto";
import { create, load, initSyncState } from "../nodejs/automerge_wasm.cjs";
import type {
  Automerge,
  SignatureBatchOutcome,
  SignatureInsertion,
  SigningRequest,
} from "../nodejs/automerge_wasm.cjs";

const subtle = webcrypto.subtle;

const SIGNING_PREFIX = "automerge-change-signature-v1";

type Signer = {
  author: string;
  sign: (data: Uint8Array) => Promise<Uint8Array>;
};

async function makeSigner(): Promise<Signer> {
  const keys = (await subtle.generateKey("Ed25519", true, [
    "sign",
    "verify",
  ])) as CryptoKeyPair;
  const rawPub = new Uint8Array(await subtle.exportKey("raw", keys.publicKey));
  return {
    author: Buffer.from(rawPub).toString("hex"),
    sign: async (data) =>
      new Uint8Array(
        await subtle.sign("Ed25519", keys.privateKey, Buffer.from(data)),
      ),
  };
}

async function signAll(requests: SigningRequest[], signer: Signer) {
  const inputs = [];
  for (const { message } of requests) {
    inputs.push({ message, signature: await signer.sign(message) });
  }
  return inputs;
}

function completed(outcome: SignatureBatchOutcome): SignatureInsertion[] {
  assert.equal(outcome.status, "completed", JSON.stringify(outcome));
  if (outcome.status !== "completed") throw new Error("unreachable");
  return outcome.insertions;
}

function messageFor(hash: string): Uint8Array {
  const prefix = Buffer.from(SIGNING_PREFIX, "utf8");
  return new Uint8Array(Buffer.concat([prefix, Buffer.from(hash, "hex")]));
}

/** An attested document with `values.length` committed, unsigned local changes. */
async function draftingDoc(values: number[], actor?: string) {
  const signer = await makeSigner();
  const doc = create({ attested: true, author: signer.author, actor });
  const heads: string[] = [];
  for (const value of values) {
    doc.put("/", `k${value}`, value);
    const head = doc.commit(`commit ${value}`, value);
    assert.ok(head);
    heads.push(head);
  }
  return { signer, doc, heads };
}

/** Sign and insert every outstanding request, asserting a completed batch. */
async function publish(doc: Automerge, signer: Signer) {
  completed(doc.insertSignatures(await signAll(doc.signingRequests(), signer)));
}

/** A one-commit attested document plus a valid signature input for it. */
async function docWithOneInput() {
  const { signer, doc, heads } = await draftingDoc([1]);
  const [good] = await signAll(doc.signingRequests(), signer);
  return { doc, good, head: heads[0] };
}

/** A fully published attested document with one commit. */
async function publishedDoc(): Promise<Automerge> {
  const { signer, doc } = await draftingDoc([1]);
  await publish(doc, signer);
  return doc;
}

/** An empty attested receiver with a fresh author. */
async function receiverDoc(): Promise<Automerge> {
  return create({ attested: true, author: (await makeSigner()).author });
}

/** `{...meta, bytes}` fragments, optionally restricted to one level. */
function exportFragments(doc: Automerge, selector?: never) {
  const meta = doc.getFragmentMetadata(selector);
  const bytes = doc.bundleFragmentMetadata(meta);
  return meta.map((m, i) => ({ ...m, bytes: bytes[i] }));
}

/** `{head, parents, bytes}` commits from the level-0 metadata. */
function exportCommits(doc: Automerge) {
  const fs = exportFragments(doc, 0 as never);
  return fs.map((f) => ({ head: f.head, parents: f.boundary, bytes: f.bytes }));
}

describe("attested option plumbing", () => {
  it("create rejects attestation without an author", () => {
    assert.throws(() => create({ attested: true }), RangeError);
  });

  it("create rejects an author that is not a usable key", () => {
    assert.throws(
      () => create({ attested: true, author: "010203" }),
      RangeError,
    );
  });

  it("create accepts a usable author and fixes it on the document", async () => {
    const { author } = await makeSigner();
    assert.equal(create({ attested: true, author }).getAuthor(), author);
  });

  it("create rejects a non-boolean attested option", () => {
    assert.throws(
      () => create({ attested: "yes" as unknown as boolean }),
      RangeError,
    );
  });

  it("load rejects attestation without an author", () => {
    assert.throws(() => load(new Uint8Array(0), { attested: true }), RangeError);
  });

  it("load accepts a usable author and fixes it on the document", async () => {
    const { author } = await makeSigner();
    const doc = load(new Uint8Array(0), { attested: true, author });
    assert.equal(doc.getAuthor(), author);
  });
});

describe("signingRequests()", () => {
  it("returns one {hash, message} entry per unsigned local commit", async () => {
    const { doc, heads } = await draftingDoc([1, 2]);
    const requests = doc.signingRequests();
    assert.deepEqual(requests.map((r) => r.hash).sort(), [...heads].sort());
    for (const request of requests) {
      assert.ok(request.message instanceof Uint8Array);
      assert.equal(request.message.length, 61);
    }
  });

  it("builds each message as the signing prefix followed by the hash bytes", async () => {
    const { doc } = await draftingDoc([1]);
    const [request] = doc.signingRequests();
    assert.deepEqual(request.message, messageFor(request.hash));
  });

  it("returns an owned snapshot unaffected by later commits", async () => {
    const { doc } = await draftingDoc([1]);
    const requests = doc.signingRequests();
    const snapshotHashes = requests.map((r) => r.hash);
    doc.put("/", "extra", true);
    doc.commit("extra", 5);
    assert.deepEqual(
      requests.map((r) => r.hash),
      snapshotHashes,
    );
    assert.equal(doc.signingRequests().length, 2);
  });

  it("throws on an unattested document without closing the pending transaction", () => {
    const unattested = create({ actor: "09" });
    unattested.put("/", "u", 99);
    assert.throws(() => unattested.signingRequests(), RangeError);
    assert.equal(unattested.pendingOps(), 1);
  });
});

describe("insertSignatures()", () => {
  it("returns a completed outcome with one insertion per input", async () => {
    const { signer, doc } = await draftingDoc([1, 2]);
    const requests = doc.signingRequests();
    const inputs = await signAll(requests, signer);
    const insertions = completed(doc.insertSignatures(inputs));
    assert.equal(insertions.length, 2);
    for (const [i, insertion] of insertions.entries()) {
      assert.equal(insertion.hash, requests[i].hash);
      assert.deepEqual(insertion.message, inputs[i].message);
      assert.deepEqual(insertion.signature, inputs[i].signature);
      assert.deepEqual(insertion.result, { ok: true, insert: "stored" });
    }
  });

  it("returns insertion bytes as copies of the caller's buffers", async () => {
    const { doc, good } = await docWithOneInput();
    const [insertion] = completed(doc.insertSignatures([good]));
    assert.notStrictEqual(insertion.message, good.message);
    assert.notStrictEqual(insertion.signature, good.signature);
  });

  it("reports a repeated entry within one batch as a duplicate", async () => {
    const { signer, doc } = await draftingDoc([1]);
    const inputs = await signAll(doc.signingRequests(), signer);
    const insertions = completed(doc.insertSignatures([inputs[0], inputs[0]]));
    assert.deepEqual(insertions[0].result, { ok: true, insert: "stored" });
    assert.deepEqual(insertions[1].result, { ok: true, insert: "duplicate" });
  });

  it("reports a per-entry verification error and still processes later entries", async () => {
    const { signer, doc } = await draftingDoc([1, 2]);
    const mallory = await makeSigner();
    const [r1, r2] = doc.signingRequests();
    const insertions = completed(
      doc.insertSignatures([
        { message: r1.message, signature: await mallory.sign(r1.message) },
        { message: r2.message, signature: await signer.sign(r2.message) },
      ]),
    );
    assert.equal(insertions[0].result.ok, false);
    assert.match(
      (insertions[0].result as { error: string }).error,
      /verification/,
    );
    assert.deepEqual(insertions[1].result, { ok: true, insert: "stored" });
  });

  it("echoes the hash of an entry whose change is not applied", async () => {
    const { signer, doc } = await draftingDoc([1]);
    const unknown = messageFor("ee".repeat(32));
    const [insertion] = completed(
      doc.insertSignatures([
        { message: unknown, signature: await signer.sign(unknown) },
      ]),
    );
    assert.equal(insertion.hash, "ee".repeat(32));
    assert.equal(insertion.result.ok, false);
    assert.match((insertion.result as { error: string }).error, /not applied/);
  });

  it("reports an empty batch as completed", async () => {
    const { doc } = await draftingDoc([]);
    assert.deepEqual(doc.insertSignatures([]), {
      status: "completed",
      insertions: [],
    });
  });

  it("rejects a malformed entry without processing the batch or closing the transaction", async () => {
    const { doc, good, head } = await docWithOneInput();
    doc.put("/", "b", 2);
    assert.equal(doc.pendingOps(), 1);
    const malformed: unknown[] = [
      [good, { message: new Uint8Array(61), signature: good.signature }],
      [
        good,
        { message: good.message.subarray(0, 40), signature: good.signature },
      ],
      [good, { message: good.message, signature: new Uint8Array(63) }],
      [good, { message: "nope", signature: good.signature }],
      [good, null],
    ];
    for (const inputs of malformed) {
      const outcome = doc.insertSignatures(inputs as never);
      assert.equal(outcome.status, "rejected", JSON.stringify(outcome));
      if (outcome.status === "rejected")
        assert.match(outcome.reason, /index 1/);
      assert.equal(doc.pendingOps(), 1);
    }
    // The good entry at index 0 was never processed.
    const outstanding = doc.signingRequests().map((r) => r.hash);
    assert.equal(outstanding.length, 2);
    assert.ok(outstanding.includes(head));
  });

  it("rejects an input that is not an array", async () => {
    const { doc } = await draftingDoc([1]);
    const outcome = doc.insertSignatures("not an array" as never);
    assert.equal(outcome.status, "rejected");
    if (outcome.status === "rejected") assert.match(outcome.reason, /array/);
  });

  it("rejects an oversized buffer by its reported length", async () => {
    const { doc, good } = await docWithOneInput();
    // Over-length buffers are rejected by their reported length before any
    // copy is made; a small buffer is enough to exercise that path.
    const huge = new Uint8Array(4096);
    const prefix = good.message.length - 32;
    for (const [bad, found] of [
      // The message error reports the hash-suffix length past the prefix.
      [{ message: huge, signature: good.signature }, 4096 - prefix],
      [{ message: good.message, signature: huge }, 4096],
    ] as const) {
      const outcome = doc.insertSignatures([good, bad]);
      assert.equal(outcome.status, "rejected");
      if (outcome.status === "rejected")
        assert.match(outcome.reason, new RegExp(`index 1.*found ${found}$`));
    }
    assert.equal(doc.hasUnpublishedChanges(), true);
  });

  it("rejects a batch on an unattested document without closing the transaction", async () => {
    const { good } = await docWithOneInput();
    const unattested = create({ actor: "09" });
    unattested.put("/", "u", 99);
    assert.equal(unattested.insertSignatures([good]).status, "rejected");
    assert.equal(unattested.pendingOps(), 1);
    assert.equal(unattested.hasUnpublishedChanges(), false);
  });

  // Only the marshalled shape of the `publicationFailed` arm belongs here; the
  // choreography that produces it is pinned by batch.rs::
  // simultaneously_admissible_pending_equivocation_fails_publication_but_retains_state.
  // The `publicationFailed` arm needs two equivocating pending groups to
  // trigger; that scenario is pinned in Rust by
  // `attestation::tests::batch::simultaneously_admissible_pending_equivocation_fails_publication_but_retains_state`.
});

describe("entry points on an attested document", () => {
  let doc: Automerge;
  let head: string;
  let unattestedPeer: Automerge;

  before(async () => {
    const drafting = await draftingDoc([1]);
    doc = drafting.doc;
    head = drafting.heads[0];
    await publish(doc, drafting.signer);
    unattestedPeer = create({ actor: "09" });
    unattestedPeer.applyChanges(doc.getChanges([]));
  });

  it("applyChanges rejects a bare change", () => {
    const raw = doc.getChangeByHash(head);
    assert.ok(raw);
    assert.throws(() => doc.applyChanges([raw]), RangeError);
  });

  it("loadIncremental rejects a bare change chunk", () => {
    const raw = doc.getChangeByHash(head);
    assert.ok(raw);
    assert.throws(() => doc.loadIncremental(raw), RangeError);
  });

  it("merge rejects an unattested document", () => {
    assert.throws(() => doc.merge(unattestedPeer), RangeError);
  });

  it("merge accepts another attested document and returns the new heads", async () => {
    const { signer, doc: peer } = await draftingDoc([7], "02");
    await publish(peer, signer);
    const merged = peer.merge(doc);
    assert.deepEqual(merged.sort(), peer.getHeads().sort());
    assert.ok(merged.includes(head));
  });

  it("generateSyncMessage rejects an attested document", () => {
    assert.throws(() => doc.generateSyncMessage(initSyncState()), RangeError);
  });

  it("receiveSyncMessage rejects an attested document", () => {
    const msg = unattestedPeer.generateSyncMessage(initSyncState());
    assert.ok(msg != null);
    assert.throws(
      () => doc.receiveSyncMessage(initSyncState(), msg),
      RangeError,
    );
  });

  it("fork rejects explicit heads", () => {
    assert.throws(() => doc.fork(undefined, [head]), RangeError);
  });

  it("fork at current heads inherits attestation", () => {
    assert.deepEqual(doc.fork("02").signingRequests(), []);
  });

  it("load rejects a signed snapshot without attested options", () => {
    assert.throws(() => load(doc.save()), RangeError);
  });
});

describe("end to end", () => {
  it("signs, publishes and transfers history to peers", async () => {
    const { signer, doc } = await draftingDoc([1, 2]);
    assert.equal(doc.hasUnpublishedChanges(), true);
    await publish(doc, signer);
    assert.deepEqual(doc.signingRequests(), []);
    assert.equal(doc.hasUnpublishedChanges(), false);
    const content = { k1: 1, k2: 2 };

    const viaCommits = await receiverDoc();
    viaCommits.addCommits(exportCommits(doc));
    assert.deepEqual(viaCommits.getHeads(), doc.getHeads());
    assert.deepEqual(viaCommits.materialize("/"), content);
    assert.equal(viaCommits.hasUnpublishedChanges(), false);

    const viaFragments = await receiverDoc();
    viaFragments.addFragments(exportFragments(doc));
    assert.deepEqual(viaFragments.getHeads(), doc.getHeads());
    assert.deepEqual(viaFragments.materialize("/"), content);

    const wire = Buffer.concat(exportFragments(doc).map((f) => f.bytes));
    const loaded = load(wire, { attested: true, author: signer.author });
    assert.deepEqual(loaded.getHeads(), doc.getHeads());
    assert.deepEqual(loaded.materialize("/"), content);
  });

  it("addFragments rejects a fragment with an unknown boundary", async () => {
    const [fragment] = exportFragments(await publishedDoc());
    const target = await receiverDoc();
    assert.throws(
      () => target.addFragments([{ ...fragment, boundary: ["ff".repeat(32)] }]),
      RangeError,
    );
    assert.deepEqual(target.getHeads(), []);
  });

  it("addCommits rejects a commit with an unknown head", async () => {
    const [commit] = exportCommits(await publishedDoc());
    const target = await receiverDoc();
    assert.throws(
      () => target.addCommits([{ ...commit, head: "ff".repeat(32) }]),
      RangeError,
    );
    assert.deepEqual(target.getHeads(), []);
  });
});
