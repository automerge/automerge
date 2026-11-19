import { describe, it } from "mocha";
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

async function makeSigner() {
  const keys = (await subtle.generateKey("Ed25519", true, [
    "sign",
    "verify",
  ])) as CryptoKeyPair;
  const rawPub = new Uint8Array(await subtle.exportKey("raw", keys.publicKey));
  const author = Buffer.from(rawPub).toString("hex");
  async function sign(data: Uint8Array): Promise<Uint8Array> {
    const sig = await subtle.sign(
      "Ed25519",
      keys.privateKey,
      Buffer.from(data),
    );
    return new Uint8Array(sig);
  }
  return { author, sign };
}

async function signAll(
  requests: SigningRequest[],
  sign: (data: Uint8Array) => Promise<Uint8Array>,
) {
  const inputs = [];
  for (const request of requests) {
    inputs.push({
      message: request.message,
      signature: await sign(request.message),
    });
  }
  return inputs;
}

function completed(outcome: SignatureBatchOutcome): SignatureInsertion[] {
  assert.equal(outcome.status, "completed", JSON.stringify(outcome));
  if (outcome.status !== "completed") throw new Error("unreachable");
  return outcome.insertions;
}

function messageFor(hash: string): Uint8Array {
  return new Uint8Array(
    Buffer.concat([
      Buffer.from(SIGNING_PREFIX, "utf8"),
      Buffer.from(hash, "hex"),
    ]),
  );
}

/** Commit until the new head hash has (or lacks) a leading "00" byte. */
function commitAtLevel(
  doc: Automerge,
  key: string,
  value: number,
  wantLevel1: boolean,
  startTime: number,
): Automerge {
  let time = startTime;
  for (;;) {
    const trial = doc.clone();
    trial.put("/", key, value);
    const h = trial.commit(`step ${value}`, time);
    assert.ok(h);
    if (h.startsWith("00") === wantLevel1) {
      doc.free();
      return trial;
    }
    trial.free();
    time++;
  }
}

describe("Automerge Wasm fragment signing", function () {
  // Mining a level-1 hash in a debug build can take a while.
  this.timeout(20000);

  it("yields owned signing requests, publishes through batch insertion, exports proof-bearing fragments, and round-trips through load and addFragments/addCommits", async () => {
    const alice = await makeSigner();
    let doc = create({ signing: true, author: alice.author });

    // Commit 1 (level 0), commit 2 (mined to level >= 1 so commits 1..2 form a
    // multi-member fragment), and commit 3 (trailing level-0 loose commit).
    doc = commitAtLevel(doc, "counter", 1, false, 1);
    doc = commitAtLevel(doc, "counter", 2, true, 1000);
    doc = commitAtLevel(doc, "counter", 3, false, 100000);
    const counter = 3;

    assert.equal(doc.getFragmentMetadata().length, 0);
    assert.equal(doc.hasUnpublishedChanges(), true);
    // Requests cover canonical frontiers: the head of fragment {1,2} plus the
    // loose commit 3. Interior member 1 is never requested.
    const requests = doc.requestSignatures();
    assert.equal(requests.length, 2);
    const requested = requests.length;
    for (const request of requests) {
      assert.equal(request.author, alice.author);
      assert.equal(request.message.length, 61);
      assert.equal(
        Buffer.from(request.message.subarray(0, 29)).toString("utf8"),
        SIGNING_PREFIX,
      );
      assert.equal(
        Buffer.from(request.message.subarray(29)).toString("hex"),
        request.hash,
      );
    }
    // Requests are an owned snapshot: later mutation does not change them.
    const snapshotHashes = requests.map(r => r.hash);
    doc.put("/", "extra", true);
    doc.commit("extra", 5);
    assert.deepEqual(
      requests.map(r => r.hash),
      snapshotHashes,
    );
    assert.equal(doc.requestSignatures().length, requested + 1);

    const inputs = await signAll(requests, alice.sign);
    // Duplicate within one batch gets its own owned outcome.
    inputs.push(inputs[0]);
    const insertions = completed(doc.insertSignatures(inputs));
    assert.equal(insertions.length, requested + 1);
    for (const [i, insertion] of insertions.entries()) {
      assert.equal(
        Buffer.from(insertion.message).toString("hex"),
        Buffer.from(inputs[i].message).toString("hex"),
      );
      assert.equal(insertion.hash, requests[i % requested].hash);
      assert.deepEqual(insertion.signature, inputs[i].signature);
      // Returned bytes are owned copies.
      assert.notStrictEqual(insertion.message, inputs[i].message);
      assert.notStrictEqual(insertion.signature, inputs[i].signature);
    }
    assert.deepEqual(insertions.slice(0, requested).map(i => i.result), [
      { ok: true, insert: "stored" },
      { ok: true, insert: "stored" },
    ]);
    assert.deepEqual(insertions[requested].result, {
      ok: true,
      insert: "duplicate",
    });
    // The stale snapshot's responses were accepted; only the extra commit remains.
    const remaining = doc.requestSignatures();
    assert.equal(remaining.length, 1);
    assert.equal(doc.hasUnpublishedChanges(), true);
    completed(doc.insertSignatures(await signAll(remaining, alice.sign)));
    assert.deepEqual(doc.requestSignatures(), []);
    assert.equal(doc.hasUnpublishedChanges(), false);

    const commitMeta = doc.getFragmentMetadata(0);
    const commitBytes = doc.bundleFragmentMetadata(commitMeta);
    const commits = commitMeta.map((meta, i) => ({
      head: meta.head,
      parents: meta.boundary,
      bytes: commitBytes[i],
    }));

    const fragmentMeta = doc.getFragmentMetadata({ start: 1 });
    const fragmentBytes = doc.bundleFragmentMetadata(fragmentMeta);
    const fragments = fragmentMeta.map((meta, i) => ({
      ...meta,
      bytes: fragmentBytes[i],
    }));

    assert.ok(fragments.length >= 1);
    assert.ok(commits.length >= 1);
    assert.ok(fragments.length + commits.length < counter + 1);

    const receiver = await makeSigner();
    const target = create({ signing: true, author: receiver.author });
    assert.throws(
      () =>
        target.addFragments([{ ...fragments[0], boundary: ["ff".repeat(32)] }]),
      RangeError,
    );
    assert.deepEqual(target.getHeads(), []);

    target.addFragments(fragments);
    assert.throws(
      () => target.addCommits([{ ...commits[0], head: "ff".repeat(32) }]),
      RangeError,
    );
    target.addCommits(commits);
    assert.deepEqual(target.getHeads(), doc.getHeads());
    assert.deepEqual(target.materialize("/"), { counter, extra: true });
    assert.equal(target.hasUnpublishedChanges(), false);

    const allMeta = doc.getFragmentMetadata();
    const allChunks = doc.bundleFragmentMetadata(allMeta);
    const wire = Buffer.concat(allChunks);
    const loaded = load(wire, { signing: true, author: receiver.author });
    assert.deepEqual(loaded.getHeads(), doc.getHeads());
    assert.deepEqual(loaded.materialize("/"), { counter, extra: true });
  });

  it("rejects a malformed batch atomically before closing the pending transaction, and reports per-entry crypto errors without stopping", async () => {
    const alice = await makeSigner();
    const mallory = await makeSigner();
    const doc = create({ signing: true, author: alice.author });
    doc.put("/", "a", 1);
    const h1 = doc.commit("first", 1);
    assert.ok(h1);
    const [request] = doc.requestSignatures();
    const good = { message: request.message, signature: await alice.sign(request.message) };

    // Open a transaction; a malformed batch must leave it pending.
    doc.put("/", "b", 2);
    assert.equal(doc.pendingOps(), 1);
    const malformed: Array<[unknown, RegExp]> = [
      [[good, { message: new Uint8Array(61), signature: good.signature }], /index 1/],
      [[good, { message: request.message.subarray(0, 40), signature: good.signature }], /index 1/],
      [[good, { message: request.message, signature: new Uint8Array(63) }], /index 1/],
      [[good, { message: "nope", signature: good.signature }], /index 1/],
      [[good, null], /index 1/],
      ["not an array", /array/],
    ];
    for (const [inputs, reason] of malformed) {
      const outcome = doc.insertSignatures(inputs as never);
      assert.equal(outcome.status, "rejected", JSON.stringify(outcome));
      if (outcome.status === "rejected") assert.match(outcome.reason, reason);
      // Still pending: nothing was committed or published.
      assert.equal(doc.pendingOps(), 1);
      assert.equal(doc.hasUnpublishedChanges(), true);
    }
    // The valid entry in a rejected batch was not processed: committing the
    // pending edit yields two outstanding requests, h1 among them.
    const afterRejections = doc.requestSignatures();
    assert.equal(doc.pendingOps(), 0);
    assert.deepEqual(
      afterRejections.map(r => r.hash).includes(h1),
      true,
    );
    assert.equal(afterRejections.length, 2);
    assert.equal(doc.getFragmentMetadata().length, 0);

    // Wrong mode is rejected before the pending transaction is closed.
    const unsigned = create({ actor: "09" });
    unsigned.put("/", "u", 99);
    const rejected = unsigned.insertSignatures([good]);
    assert.equal(rejected.status, "rejected");
    assert.equal(unsigned.pendingOps(), 1);
    assert.throws(() => unsigned.requestSignatures(), RangeError);
    assert.equal(unsigned.pendingOps(), 1);
    assert.equal(unsigned.hasUnpublishedChanges(), false);

    // Well-formed but invalid entries fail individually; later entries proceed.
    const [r1, r2] = doc.requestSignatures();
    const unknown = messageFor("ee".repeat(32));
    const wrongKey = await mallory.sign(r1.message);
    const insertions = completed(
      doc.insertSignatures([
        { message: r1.message, signature: wrongKey },
        { message: unknown, signature: await alice.sign(unknown) },
        { message: r2.message, signature: await alice.sign(r2.message) },
        { message: r1.message, signature: await alice.sign(r1.message) },
      ]),
    );
    assert.equal(insertions.length, 4);
    assert.equal(insertions[0].result.ok, false);
    assert.match((insertions[0].result as { error: string }).error, /verification/);
    assert.equal(insertions[1].result.ok, false);
    assert.match((insertions[1].result as { error: string }).error, /not applied/);
    assert.equal(insertions[1].hash, "ee".repeat(32));
    assert.deepEqual(insertions[2].result, { ok: true, insert: "stored" });
    assert.deepEqual(insertions[3].result, { ok: true, insert: "stored" });
    assert.deepEqual(doc.requestSignatures(), []);
    assert.equal(doc.hasUnpublishedChanges(), false);

    // An empty batch still runs completion and is reported as completed.
    assert.deepEqual(doc.insertSignatures([]), {
      status: "completed",
      insertions: [],
    });
  });

  it("stages out-of-order fragments and rejects snapshots, unauthenticated entry points, and signed sync without mutating state", async () => {
    const alice = await makeSigner();
    const mallory = await makeSigner();
    const doc = create({ signing: true, author: alice.author });

    doc.put("/", "a", 1);
    const h1 = doc.commit("first", 1);
    doc.put("/", "b", 2);
    const h2 = doc.commit("second", 2);
    assert.ok(h1);
    assert.ok(h2);
    completed(doc.insertSignatures(await signAll(doc.requestSignatures(), alice.sign)));

    const meta = doc.getFragmentMetadata(0);
    const bytes = doc.bundleFragmentMetadata(meta);
    const commits = meta.map((m, i) => ({
      head: m.head,
      parents: m.boundary,
      bytes: bytes[i],
    }));
    const ordered = [...commits].sort(
      (a, b) => a.parents.length - b.parents.length,
    );

    // Out-of-order arrival across separate calls stays pending until causal parent arrives.
    const staged = create({ signing: true, author: mallory.author });
    staged.addCommits([ordered[1]]);
    assert.deepEqual(staged.getHeads(), []);
    staged.addCommits([ordered[0]]);
    assert.deepEqual(staged.getHeads(), [h2]);
    assert.deepEqual(staged.materialize("/"), { a: 1, b: 2 });

    // Invalid signature or bare change chunk fails without mutating receiver.
    const rawChange = doc.getChangeByHash(h1);
    assert.ok(rawChange);
    assert.throws(() => staged.loadIncremental(rawChange), RangeError);
    assert.throws(() => staged.applyChanges([rawChange]), RangeError);
    const unsignedCopy = create();
    unsignedCopy.applyChanges(doc.getChanges([]));
    assert.throws(() => staged.merge(unsignedCopy), RangeError);
    assert.throws(() => unsignedCopy.merge(staged), RangeError);
    assert.throws(
      () => load(unsignedCopy.save(), { signing: true, author: mallory.author }),
      RangeError,
    );
    assert.throws(() => load(doc.save()), RangeError);
    assert.throws(() => doc.fork(undefined, [h1]), RangeError);

    // Fork at current heads remains allowed and inherits signing mode.
    const forked = doc.fork("02");
    assert.deepEqual(forked.requestSignatures(), []);

    // Signed sync is an explicit error in both directions and leaves state alone.
    const unsignedPeer = create({ actor: "09" });
    unsignedPeer.put("/", "u", 99);
    unsignedPeer.commit();
    const peerSync = initSyncState();
    const syncMsg = unsignedPeer.generateSyncMessage(peerSync);
    assert.ok(syncMsg != null);

    const signingSync = initSyncState();
    assert.throws(() => staged.generateSyncMessage(signingSync), RangeError);
    assert.throws(
      () => staged.receiveSyncMessage(signingSync, syncMsg),
      RangeError,
    );
    assert.deepEqual(staged.getHeads(), [h2]);

    assert.throws(
      () => create({ signing: "yes" as unknown as boolean }),
      RangeError,
    );
  });

  it("merges only published history with proofs, preserving receiver drafts and deduplicating", async () => {
    const alice = await makeSigner();
    const bob = await makeSigner();
    const source = create({ signing: true, author: alice.author, actor: "01" });
    source.put("/", "published", 1);
    const published = source.commit("published", 1)!;
    completed(source.insertSignatures(await signAll(source.requestSignatures(), alice.sign)));
    source.put("/", "draft", 2);
    const draft = source.commit("draft", 2)!;

    const receiver = create({ signing: true, author: bob.author, actor: "02" });
    receiver.put("/", "mine", 3);
    const mine = receiver.commit("mine", 3)!;

    const heads = receiver.merge(source);
    assert.deepEqual(heads.sort(), [mine, published].sort());
    assert.deepEqual(receiver.getHeads().sort(), [mine, published].sort());
    assert.deepEqual(receiver.materialize("/"), { published: 1, mine: 3 });
    assert.equal(receiver.getChangeByHash(draft), null);
    // Source is intact: draft still applied, still requested.
    assert.deepEqual(source.getHeads(), [draft]);
    assert.equal(source.requestSignatures().length, 1);
    // Repeated merge is a no-op.
    assert.deepEqual(receiver.merge(source).sort(), [mine, published].sort());
    assert.deepEqual(receiver.getChanges([]).length, 2);
    // Merged proof is re-exportable to an empty peer.
    const wire = Buffer.concat(
      receiver.bundleFragmentMetadata(receiver.getFragmentMetadata()),
    );
    const empty = load(wire, { signing: true, author: bob.author });
    assert.deepEqual(empty.getHeads(), [published]);
  });

  it("saves only published history and later emits published drafts incrementally", async () => {
    const alice = await makeSigner();
    const doc = create({ signing: true, author: alice.author, actor: "01" });
    doc.put("/", "a", 1);
    const a = doc.commit("a", 1)!;
    completed(doc.insertSignatures(await signAll(doc.requestSignatures(), alice.sign)));
    doc.put("/", "b", 2);
    const b = doc.commit("b", 2)!;

    const snapshot = doc.save();
    const reloaded = load(snapshot, { signing: true, author: alice.author });
    assert.deepEqual(reloaded.getHeads(), [a]);
    assert.deepEqual(reloaded.materialize("/"), { a: 1 });
    assert.equal(reloaded.hasUnpublishedChanges(), false);
    // Reload creates no signing eligibility for imported history.
    assert.deepEqual(reloaded.requestSignatures(), []);
    // The draft stays live in the original document.
    assert.deepEqual(doc.getHeads(), [b]);
    assert.equal(doc.hasUnpublishedChanges(), true);

    assert.equal(doc.saveIncremental().length, 0);
    completed(doc.insertSignatures(await signAll(doc.requestSignatures(), alice.sign)));
    const increment = doc.saveIncremental();
    assert.ok(increment.length > 0);
    reloaded.loadIncremental(increment);
    assert.deepEqual(reloaded.getHeads(), [b]);
    assert.deepEqual(reloaded.materialize("/"), { a: 1, b: 2 });
  });

  it("requires a usable author to create or load, and guards new-author forks", async () => {
    const alice = await makeSigner();
    const bob = await makeSigner();
    const weak = "01" + "00".repeat(31);

    // Construction without an author or with an unusable key is rejected.
    assert.throws(() => create({ signing: true }), RangeError);
    assert.throws(
      () => create({ signing: true, author: "010203" }),
      RangeError,
    );
    assert.throws(() => create({ signing: true, author: weak }), RangeError);
    assert.equal(
      (create as unknown as { setAuthor?: unknown }).setAuthor,
      undefined,
    );

    const doc = create({ signing: true, author: alice.author });
    assert.equal(
      (doc as unknown as { setAuthor?: unknown }).setAuthor,
      undefined,
    );
    const empty = new Uint8Array(0);
    assert.throws(() => load(empty, { signing: true }), RangeError);
    assert.throws(
      () => load(empty, { signing: true, author: weak }),
      RangeError,
    );
    assert.equal(
      load(empty, { signing: true, author: alice.author }).getAuthor(),
      alice.author,
    );

    // Author is fixed per instance; a new-author fork validates its key and
    // leaves the source document unchanged.
    assert.throws(() => doc.forkWithAuthor(null), RangeError);
    assert.throws(() => doc.forkWithAuthor("010203"), RangeError);
    assert.equal(doc.getAuthor(), alice.author);

    // With an unpublished draft (even a pending transaction) a different
    // author is rejected and the source keeps its open transaction.
    doc.put("/", "x", 1);
    assert.equal(doc.pendingOps(), 1);
    assert.throws(() => doc.forkWithAuthor(bob.author), RangeError);
    assert.equal(doc.pendingOps(), 1);
    // A malformed actor is rejected before the source transaction is closed.
    assert.throws(() => doc.forkWithAuthor(alice.author, "zz"), RangeError);
    assert.equal(doc.pendingOps(), 1);
    // Same author is always allowed.
    const same = doc.forkWithAuthor(alice.author, "0a");
    assert.equal(same.getAuthor(), alice.author);
    assert.equal(same.getActorId(), "0a");
    assert.equal(doc.pendingOps(), 0);
    const [draft] = doc.getHeads();
    assert.throws(() => doc.forkWithAuthor(bob.author), RangeError);

    // Once published, a different author may fork with a fresh actor.
    completed(doc.insertSignatures(await signAll(doc.requestSignatures(), alice.sign)));
    const forked = doc.forkWithAuthor(bob.author);
    assert.equal(forked.getAuthor(), bob.author);
    assert.notEqual(forked.getActorId(), doc.getActorId());
    assert.deepEqual(forked.getHeads(), [draft]);
    assert.deepEqual(forked.requestSignatures(), []);
    assert.equal(forked.getAuthorForActor(doc.getActorId()), alice.author);
    assert.equal(doc.getAuthor(), alice.author);

    // Unsigned documents stay permissive.
    const unsigned = create({ author: "010203" });
    assert.equal(unsigned.getAuthor(), "010203");
    assert.equal(unsigned.forkWithAuthor(null).getAuthor(), null);
    assert.equal(create().getAuthor(), null);
  });
});
