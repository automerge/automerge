import { describe, it } from "mocha";
import assert from "assert";
import { create, load, initSyncState } from "../nodejs/automerge_wasm.cjs";

const CHANGE_ID = /^\d+@[0-9a-f]+$/;
const HASH = /^[0-9a-f]{64}$/;

const DOCUMENT_CHUNK = 0;
const CHANGE_CHUNK = 1;
const CHANGE_SET_CHUNK = 4;

function chunkType(bytes: Uint8Array): number {
  // magic bytes, then a four byte checksum, then the chunk type
  return bytes[8];
}

function docWithHistory() {
  const doc = create({ actor: "aaaa" });
  doc.put("_root", "a", 1);
  doc.commit();
  doc.put("_root", "b", 2);
  doc.commit();
  return doc;
}

describe("Automerge", () => {
  describe("enableAuditMode", () => {
    it("names heads by hash in audit mode and by change id outside it", () => {
      const doc = docWithHistory();
      const hashes = doc.getHeads();
      assert.match(hashes[0], HASH);

      doc.enableAuditMode(false);
      assert.deepStrictEqual(doc.getHeads(), ["2@aaaa"]);

      doc.enableAuditMode(true);
      assert.deepStrictEqual(doc.getHeads(), hashes);
    });

    it("returns change ids from commit, emptyChange and merge", () => {
      const doc = docWithHistory();
      doc.enableAuditMode(false);
      doc.put("_root", "c", 3);
      assert.strictEqual(doc.commit(), "3@aaaa");
      assert.strictEqual(doc.emptyChange(), "4@aaaa");

      const other = doc.fork("bbbb");
      other.put("_root", "d", 4);
      other.commit();
      // the fork's change depends on 4@aaaa, so it is the only head
      assert.deepStrictEqual(doc.merge(other), ["1@bbbb"]);
    });

    it("saves in the change set format outside audit mode", () => {
      const doc = docWithHistory();
      assert.strictEqual(chunkType(doc.save()), DOCUMENT_CHUNK);

      doc.enableAuditMode(false);
      const saved = doc.save();
      assert.strictEqual(chunkType(saved), CHANGE_SET_CHUNK);

      doc.put("_root", "c", 3);
      const incremental = doc.saveIncremental();
      assert.strictEqual(chunkType(incremental), CHANGE_SET_CHUNK);

      const loaded = load(saved);
      loaded.loadIncremental(incremental);
      assert.deepStrictEqual(loaded.materialize("/"), { a: 1, b: 2, c: 3 });
    });

    it("still saves change chunks incrementally in audit mode", () => {
      const doc = docWithHistory();
      doc.save();
      doc.put("_root", "c", 3);
      assert.strictEqual(chunkType(doc.saveIncremental()), CHANGE_CHUNK);
    });

    it("reads at change id heads outside audit mode", () => {
      const doc = docWithHistory();
      doc.enableAuditMode(false);
      assert.deepStrictEqual(doc.materialize("/", ["1@aaaa"]), { a: 1 });
      assert.strictEqual(doc.get("_root", "b", ["1@aaaa"]), undefined);
      assert.deepStrictEqual(doc.keys("_root", ["2@aaaa"]), ["a", "b"]);
      const patches = doc.diff(["1@aaaa"], ["2@aaaa"]);
      assert.deepStrictEqual(patches, [
        { action: "put", path: ["b"], value: 2 },
      ]);
      const fork = doc.fork(undefined, ["1@aaaa"]);
      assert.deepStrictEqual(fork.materialize("/"), { a: 1 });
    });

    it("rejects heads in the other mode's format", () => {
      const doc = docWithHistory();
      const hashes = doc.getHeads();
      assert.throws(() => doc.keys("_root", ["2@aaaa"]));

      doc.enableAuditMode(false);
      assert.throws(() => doc.keys("_root", hashes));
    });

    it("merges documents in different modes", () => {
      const audited = docWithHistory();
      const next = audited.fork("bbbb");
      next.enableAuditMode(false);
      next.put("_root", "c", 3);
      next.commit();

      audited.merge(next);
      assert.deepStrictEqual(audited.materialize("/"), { a: 1, b: 2, c: 3 });
      assert.match(audited.getHeads()[0], HASH);
    });

    it("is chosen by the auditMode option of create and load", () => {
      const doc = create({ actor: "aaaa", auditMode: false });
      assert.strictEqual(doc.isAuditMode(), false);
      doc.put("_root", "a", 1);
      assert.strictEqual(doc.commit(), "1@aaaa");

      const saved = docWithHistory().save();
      assert.strictEqual(load(saved).isAuditMode(), true);
      const loaded = load(saved, { auditMode: false });
      assert.strictEqual(loaded.isAuditMode(), false);
      assert.deepStrictEqual(loaded.getHeads(), ["2@aaaa"]);
      assert.deepStrictEqual(loaded.materialize("/"), { a: 1, b: 2 });
    });

    it("refuses to sync outside audit mode", () => {
      const doc = docWithHistory();
      doc.enableAuditMode(false);
      assert.throws(() => doc.generateSyncMessage(initSyncState()));
      assert.throws(() => doc.hasOurChanges(initSyncState()));

      doc.enableAuditMode(true);
      assert.notStrictEqual(doc.generateSyncMessage(initSyncState()), null);
    });
  });

  describe("hasChange", () => {
    it("takes a hash in audit mode and a change id outside it", () => {
      const doc = docWithHistory();
      const [hash] = doc.getHeads();
      assert.strictEqual(doc.hasChange(hash), true);
      assert.strictEqual(doc.hasChange("0".repeat(64)), false);

      doc.enableAuditMode(false);
      assert.strictEqual(doc.hasChange("2@aaaa"), true);
      assert.strictEqual(doc.hasChange("3@aaaa"), false);
      assert.throws(() => doc.hasChange(hash));
    });
  });

  describe("getDecodedChange", () => {
    it("takes a hash or a change id in either mode", () => {
      const doc = docWithHistory();
      const [hash] = doc.getHeads();
      const byHash = doc.getDecodedChange(hash);
      assert.strictEqual(byHash?.seq, 2);
      assert.deepStrictEqual(doc.getDecodedChange("2@aaaa"), byHash);

      doc.enableAuditMode(false);
      assert.deepStrictEqual(doc.getDecodedChange("2@aaaa"), byHash);
      assert.strictEqual(doc.getDecodedChange("3@aaaa"), null);
    });
  });
});
