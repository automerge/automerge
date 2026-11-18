import { describe, it } from "mocha";
import assert from "assert";
import { webcrypto } from "node:crypto";
import { create, load, initSyncState } from "../nodejs/automerge_wasm.cjs";

const subtle = webcrypto.subtle;

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

describe("Automerge Wasm fragment signing", () => {
  it("yields locally authored signable payloads, exports proof-bearing fragments, and round-trips through load and addFragments/addCommits", async () => {
    const alice = await makeSigner();
    let doc = create({ signing: true, author: alice.author });

    // Commit 1 (level 0), commit 2 (mined to level >= 1 so commits 1..2 form a
    // multi-member fragment), and commit 3 (trailing level-0 loose commit).
    let t1 = 1;
    for (;;) {
      const trial = doc.clone();
      trial.put("/", "counter", 1);
      const h = trial.commit("step 1", t1);
      assert.ok(h);
      if (!h.startsWith("00")) {
        doc.free();
        doc = trial;
        break;
      }
      trial.free();
      t1++;
    }
    let t2 = 1000;
    for (;;) {
      const trial = doc.clone();
      trial.put("/", "counter", 2);
      const h = trial.commit("step 2", t2);
      assert.ok(h);
      if (h.startsWith("00")) {
        doc.free();
        doc = trial;
        break;
      }
      trial.free();
      t2++;
    }
    let t3 = 100000;
    for (;;) {
      const trial = doc.clone();
      trial.put("/", "counter", 3);
      const h = trial.commit("step 3", t3);
      assert.ok(h);
      if (!h.startsWith("00")) {
        doc.free();
        doc = trial;
        break;
      }
      trial.free();
      t3++;
    }
    const counter = 3;

    assert.equal(doc.getFragmentMetadata().length, 0);
    const payloads = doc.requiredSignatures();
    assert.equal(payloads.length, counter);

    for (const data of payloads) {
      assert.equal(data.length, 61);
      assert.equal(
        Buffer.from(data.subarray(0, 29)).toString("utf8"),
        "automerge-change-signature-v1",
      );
      const sig = await alice.sign(data);
      doc.insertSignature(data, sig);
      // Duplicate insertion is idempotent.
      doc.insertSignature(data, sig);
    }
    assert.deepEqual(doc.requiredSignatures(), []);

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
    assert.ok(fragments.length + commits.length < counter);

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
    assert.deepEqual(target.materialize("/"), { counter });

    const allMeta = doc.getFragmentMetadata();
    const allChunks = doc.bundleFragmentMetadata(allMeta);
    const wire = Buffer.concat(allChunks);
    const loaded = load(wire, { signing: true, author: receiver.author });
    assert.deepEqual(loaded.getHeads(), doc.getHeads());
    assert.deepEqual(loaded.materialize("/"), { counter });
  });

  it("stages out-of-order fragments and rejects snapshots, unauthenticated entry points, and invalid payloads without mutating state", async () => {
    const alice = await makeSigner();
    const mallory = await makeSigner();
    const doc = create({ signing: true, author: alice.author });

    doc.put("/", "a", 1);
    const h1 = doc.commit("first", 1);
    doc.put("/", "b", 2);
    const h2 = doc.commit("second", 2);
    assert.ok(h1);
    assert.ok(h2);

    const payloads = doc.requiredSignatures();
    assert.equal(payloads.length, 2);

    // Bad payload prefix, bad payload length, bad signature length, and wrong key all throw RangeError.
    const validSig0 = await alice.sign(payloads[0]);
    assert.throws(
      () => doc.insertSignature(new Uint8Array(61), validSig0),
      RangeError,
    );
    assert.throws(
      () => doc.insertSignature(payloads[0].subarray(0, 40), validSig0),
      RangeError,
    );
    assert.throws(
      () => doc.insertSignature(payloads[0], new Uint8Array(63)),
      RangeError,
    );
    const wrongSig = await mallory.sign(payloads[0]);
    assert.throws(() => doc.insertSignature(payloads[0], wrongSig), RangeError);

    for (const data of payloads) {
      doc.insertSignature(data, await alice.sign(data));
    }

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
    assert.throws(() => staged.merge(doc), RangeError);
    assert.throws(
      () => load(doc.save(), { signing: true, author: mallory.author }),
      RangeError,
    );
    assert.throws(() => doc.fork(undefined, [h1]), RangeError);

    // Fork at current heads remains allowed and inherits signing mode.
    const forked = doc.fork("02");
    assert.deepEqual(forked.requiredSignatures(), []);

    // Sync protocol does not admit unauthenticated changes in signing mode.
    const unsignedPeer = create({ actor: "09" });
    unsignedPeer.put("/", "u", 99);
    unsignedPeer.commit();
    const peerSync = initSyncState();
    const syncMsg = unsignedPeer.generateSyncMessage(peerSync);
    assert.ok(syncMsg != null);

    const signingSync = initSyncState();
    assert.equal(staged.generateSyncMessage(signingSync), null);
    assert.throws(
      () => staged.receiveSyncMessage(signingSync, syncMsg),
      RangeError,
    );
    assert.deepEqual(staged.getHeads(), [h2]);

    // Unsigned documents reject signing-only methods and non-boolean signing options.
    assert.throws(() => unsignedPeer.requiredSignatures(), RangeError);
    assert.throws(
      () => unsignedPeer.insertSignature(payloads[0], validSig0),
      RangeError,
    );
    assert.throws(
      () => create({ signing: "yes" as unknown as boolean }),
      RangeError,
    );
  });

  it("requires a usable author to create, load, or update a signing document", async () => {
    const alice = await makeSigner();
    const weak = "01" + "00".repeat(31);

    // Construction without an author or with an unusable key is rejected.
    assert.throws(() => create({ signing: true }), RangeError);
    assert.throws(
      () => create({ signing: true, author: "010203" }),
      RangeError,
    );
    assert.throws(() => create({ signing: true, author: weak }), RangeError);

    const doc = create({ signing: true, author: alice.author });
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

    // Author updates follow the same rule and leave the document unchanged.
    assert.throws(() => doc.setAuthor(null), RangeError);
    assert.throws(() => doc.setAuthor("010203"), RangeError);
    assert.equal(doc.getAuthor(), alice.author);

    // Unsigned documents stay permissive.
    const unsigned = create();
    unsigned.setAuthor("010203");
    unsigned.setAuthor(null);
    assert.equal(unsigned.getAuthor(), null);
  });
});
