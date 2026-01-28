import { describe, it } from "mocha";
import assert from "assert";
// @ts-ignore
import { BloomFilter } from "./helpers/sync.mjs";
import {
  create,
  load,
  SyncState,
  Automerge,
  encodeChange,
  decodeChange,
  initSyncState,
  decodeSyncMessage,
  decodeSyncState,
  encodeSyncState,
  encodeSyncMessage,
} from "../nodejs/automerge_wasm.cjs";
import { DecodedSyncMessage, Hash } from "../nodejs/automerge_wasm.cjs";

function sync(
  a: Automerge,
  b: Automerge,
  aSyncState = initSyncState(),
  bSyncState = initSyncState(),
) {
  const MAX_ITER = 10;
  let aToBmsg = null,
    bToAmsg = null,
    i = 0;
  do {
    aToBmsg = a.generateSyncMessage(aSyncState);
    bToAmsg = b.generateSyncMessage(bSyncState);

    if (aToBmsg) {
      b.receiveSyncMessage(bSyncState, aToBmsg);
    }
    if (bToAmsg) {
      a.receiveSyncMessage(aSyncState, bToAmsg);
    }

    if (i++ > MAX_ITER) {
      throw new Error(`Did not synchronize within ${MAX_ITER} iterations`);
    }
  } while (aToBmsg || bToAmsg);
}

describe("Automerge", () => {
  describe("basics", () => {
    it("should create, clone and free", () => {
      const doc1 = create();
      const doc2 = doc1.clone();
      doc2.free();
    });

    it("should be able to start and commit", () => {
      const doc = create();
      doc.commit();
    });

    it("getting a nonexistent prop does not throw an error", () => {
      const doc = create();
      const root = "_root";
      const result = doc.getWithType(root, "hello");
      assert.deepEqual(result, undefined);
    });

    it("should be able to set and get a simple value", () => {
      const doc: Automerge = create({ actor: "aabbcc" });
      const root = "_root";
      let result;

      doc.put(root, "hello", "world");
      doc.put(root, "number1", 5, "uint");
      doc.put(root, "number2", 5);
      doc.put(root, "number3", 5.5);
      doc.put(root, "number4", 5.5, "f64");
      doc.put(root, "number5", 5.5, "int");
      doc.put(root, "bool", true);
      doc.put(root, "time1", 1000, "timestamp");
      doc.put(root, "time2", new Date(1001));
      doc.putObject(root, "list", []);
      doc.put(root, "null", null);

      result = doc.getWithType(root, "hello");
      assert.deepEqual(result, ["str", "world"]);
      assert.deepEqual(doc.get("/", "hello"), "world");

      result = doc.getWithType(root, "number1");
      assert.deepEqual(result, ["uint", 5]);
      assert.deepEqual(doc.get("/", "number1"), 5);

      result = doc.getWithType(root, "number2");
      assert.deepEqual(result, ["int", 5]);

      result = doc.getWithType(root, "number3");
      assert.deepEqual(result, ["f64", 5.5]);

      result = doc.getWithType(root, "number4");
      assert.deepEqual(result, ["f64", 5.5]);

      result = doc.getWithType(root, "number5");
      assert.deepEqual(result, ["int", 5]);

      result = doc.getWithType(root, "bool");
      assert.deepEqual(result, ["boolean", true]);

      doc.put(root, "bool", false, "boolean");

      result = doc.getWithType(root, "bool");
      assert.deepEqual(result, ["boolean", false]);

      result = doc.getWithType(root, "time1");
      assert.deepEqual(result, ["timestamp", new Date(1000)]);

      result = doc.getWithType(root, "time2");
      assert.deepEqual(result, ["timestamp", new Date(1001)]);

      result = doc.getWithType(root, "list");
      assert.deepEqual(result, ["list", "10@aabbcc"]);

      result = doc.getWithType(root, "null");
      assert.deepEqual(result, ["null", null]);
    });

    it("should be able to use bytes", () => {
      const doc = create();
      doc.put("_root", "data1", new Uint8Array([10, 11, 12]));
      doc.put("_root", "data2", new Uint8Array([13, 14, 15]), "bytes");
      const value1 = doc.getWithType("_root", "data1");
      assert.deepEqual(value1, ["bytes", new Uint8Array([10, 11, 12])]);
      const value2 = doc.getWithType("_root", "data2");
      assert.deepEqual(value2, ["bytes", new Uint8Array([13, 14, 15])]);
    });

    it("should be able to make subobjects", () => {
      const doc = create();
      const root = "_root";
      let result;

      const submap = doc.putObject(root, "submap", {});
      doc.put(submap, "number", 6, "uint");
      assert.strictEqual(doc.pendingOps(), 2);

      result = doc.getWithType(root, "submap");
      assert.deepEqual(result, ["map", submap]);

      result = doc.getWithType(submap, "number");
      assert.deepEqual(result, ["uint", 6]);
    });

    it("should be able to make lists", () => {
      const doc = create();
      const root = "_root";

      const sublist = doc.putObject(root, "numbers", []);
      doc.insert(sublist, 0, "a");
      doc.insert(sublist, 1, "b");
      doc.insert(sublist, 2, "c");
      doc.insert(sublist, 0, "z");

      assert.deepEqual(doc.getWithType(sublist, 0), ["str", "z"]);
      assert.deepEqual(doc.getWithType(sublist, 1), ["str", "a"]);
      assert.deepEqual(doc.getWithType(sublist, 2), ["str", "b"]);
      assert.deepEqual(doc.getWithType(sublist, 3), ["str", "c"]);
      assert.deepEqual(doc.length(sublist), 4);

      doc.put(sublist, 2, "b v2");

      assert.deepEqual(doc.getWithType(sublist, 2), ["str", "b v2"]);
      assert.deepEqual(doc.length(sublist), 4);
    });

    it("lists have insert, set, splice, and push ops", () => {
      const doc = create();
      const root = "_root";

      const sublist = doc.putObject(root, "letters", []);
      doc.insert(sublist, 0, "a");
      doc.insert(sublist, 0, "b");
      assert.deepEqual(doc.materialize(), { letters: ["b", "a"] });
      doc.push(sublist, "c");
      const heads = doc.getHeads();
      assert.deepEqual(doc.materialize(), { letters: ["b", "a", "c"] });
      doc.push(sublist, 3, "timestamp");
      assert.deepEqual(doc.materialize(), {
        letters: ["b", "a", "c", new Date(3)],
      });
      doc.splice(sublist, 1, 1, ["d", "e", "f"]);
      assert.deepEqual(doc.materialize(), {
        letters: ["b", "d", "e", "f", "c", new Date(3)],
      });
      doc.put(sublist, 0, "z");
      assert.deepEqual(doc.materialize(), {
        letters: ["z", "d", "e", "f", "c", new Date(3)],
      });
      assert.deepEqual(doc.materialize(sublist), [
        "z",
        "d",
        "e",
        "f",
        "c",
        new Date(3),
      ]);
      assert.deepEqual(doc.length(sublist), 6);
      assert.deepEqual(doc.materialize("/", heads), {
        letters: ["b", "a", "c"],
      });
    });

    it("should be able delete non-existent props", () => {
      const doc = create();

      doc.put("_root", "foo", "bar");
      doc.put("_root", "bip", "bap");
      const hash1 = doc.commit();

      assert.deepEqual(doc.keys("_root"), ["bip", "foo"]);

      doc.delete("_root", "foo");
      doc.delete("_root", "baz");
      const hash2 = doc.commit();

      assert.deepEqual(doc.keys("_root"), ["bip"]);
      assert.ok(hash1);
      assert.deepEqual(doc.keys("_root", [hash1]), ["bip", "foo"]);
      assert.ok(hash2);
      assert.deepEqual(doc.keys("_root", [hash2]), ["bip"]);
    });

    it("should be able to del", () => {
      const doc = create();
      const root = "_root";

      doc.put(root, "xxx", "xxx");
      assert.deepEqual(doc.getWithType(root, "xxx"), ["str", "xxx"]);
      doc.delete(root, "xxx");
      assert.deepEqual(doc.getWithType(root, "xxx"), undefined);
    });

    it("should be able to use counters", () => {
      const doc = create();
      const root = "_root";

      doc.put(root, "counter", 10, "counter");
      assert.deepEqual(doc.getWithType(root, "counter"), ["counter", 10]);
      doc.increment(root, "counter", 10);
      assert.deepEqual(doc.getWithType(root, "counter"), ["counter", 20]);
      doc.increment(root, "counter", -5);
      assert.deepEqual(doc.getWithType(root, "counter"), ["counter", 15]);
    });

    it("should be able to splice text", () => {
      const doc = create();
      const root = "_root";

      const text = doc.putObject(root, "text", "");
      doc.splice(text, 0, 0, "hello ");
      doc.splice(text, 6, 0, "world");
      doc.splice(text, 11, 0, "!?");
      assert.deepEqual(doc.getWithType(text, 0), ["str", "h"]);
      assert.deepEqual(doc.getWithType(text, 1), ["str", "e"]);
      assert.deepEqual(doc.getWithType(text, 9), ["str", "l"]);
      assert.deepEqual(doc.getWithType(text, 10), ["str", "d"]);
      assert.deepEqual(doc.getWithType(text, 11), ["str", "!"]);
      assert.deepEqual(doc.getWithType(text, 12), ["str", "?"]);
    });

    it.skip("should NOT be able to insert objects into text", () => {
      const doc = create();
      const text = doc.putObject("/", "text", "Hello world");
      assert.throws(() => {
        doc.insertObject(text, 6, { hello: "world" });
      });
    });

    it("should be able save all or incrementally", () => {
      const doc = create();

      doc.put("_root", "foo", 1);

      const save1 = doc.save();

      doc.put("_root", "bar", 2);

      const saveMidway = doc.clone().save();

      const save2 = doc.saveIncremental();

      doc.put("_root", "baz", 3);

      const save3 = doc.saveIncremental();

      const saveA = doc.save();
      const saveB = new Uint8Array([...save1, ...save2, ...save3]);

      assert.notDeepEqual(saveA, saveB);

      const docA = load(saveA);
      const docB = load(saveB);
      const docC = load(saveMidway);
      docC.loadIncremental(save3);

      assert.deepEqual(docA.keys("_root"), docB.keys("_root"));
      assert.deepEqual(docA.save(), docB.save());
      assert.deepEqual(docA.save(), docC.save());
    });

    it("should be able to save since a given heads", () => {
      const doc = create();

      doc.put("_root", "foo", 1);
      const heads = doc.getHeads();
      doc.saveIncremental();

      doc.put("_root", "bar", 2);

      const saveIncremental = doc.saveIncremental();
      const saveSince = doc.saveSince(heads);
      assert.deepEqual(saveIncremental, saveSince);
    });

    it("should be able to splice text", () => {
      const doc = create();
      const text = doc.putObject("_root", "text", "");
      doc.splice(text, 0, 0, "hello world");
      const hash1 = doc.commit();
      doc.splice(text, 6, 0, "big bad ");
      const hash2 = doc.commit();
      assert.strictEqual(doc.text(text), "hello big bad world");
      assert.strictEqual(doc.length(text), 19);
      assert.ok(hash1);
      assert.strictEqual(doc.text(text, [hash1]), "hello world");
      assert.strictEqual(doc.length(text, [hash1]), 11);
      assert.ok(hash2);
      assert.strictEqual(doc.text(text, [hash2]), "hello big bad world");
      assert.ok(hash2);
      assert.strictEqual(doc.length(text, [hash2]), 19);
    });

    it("local inc increments all visible counters in a map", () => {
      const doc1 = create({ actor: "aaaa" });
      doc1.put("_root", "hello", "world");
      const doc2 = load(doc1.save(), { actor: "bbbb" });
      const doc3 = load(doc1.save(), { actor: "cccc" });
      const heads = doc1.getHeads();
      doc1.put("_root", "cnt", 20);
      doc2.put("_root", "cnt", 0, "counter");
      doc3.put("_root", "cnt", 10, "counter");
      doc1.applyChanges(doc2.getChanges(heads));
      doc1.applyChanges(doc3.getChanges(heads));
      let result = doc1.getAll("_root", "cnt");
      assert.deepEqual(result, [
        ["int", 20, "2@aaaa"],
        ["counter", 0, "2@bbbb"],
        ["counter", 10, "2@cccc"],
      ]);
      doc1.increment("_root", "cnt", 5);
      result = doc1.getAll("_root", "cnt");
      assert.deepEqual(result, [
        ["counter", 5, "2@bbbb"],
        ["counter", 15, "2@cccc"],
      ]);

      const save1 = doc1.save();
      const doc4 = load(save1);
      assert.deepEqual(doc4.save(), save1);
    });

    it("local inc increments all visible counters in a sequence", () => {
      const doc1 = create({ actor: "aaaa" });
      const seq = doc1.putObject("_root", "seq", []);
      doc1.insert(seq, 0, "hello");
      const doc2 = load(doc1.save(), { actor: "bbbb" });
      const doc3 = load(doc1.save(), { actor: "cccc" });
      const heads = doc1.getHeads();
      doc1.put(seq, 0, 20);
      doc2.put(seq, 0, 0, "counter");
      doc3.put(seq, 0, 10, "counter");
      doc1.applyChanges(doc2.getChanges(heads));
      doc1.applyChanges(doc3.getChanges(heads));
      let result = doc1.getAll(seq, 0);
      assert.deepEqual(result, [
        ["int", 20, "3@aaaa"],
        ["counter", 0, "3@bbbb"],
        ["counter", 10, "3@cccc"],
      ]);
      doc1.increment(seq, 0, 5);
      result = doc1.getAll(seq, 0);
      assert.deepEqual(result, [
        ["counter", 5, "3@bbbb"],
        ["counter", 15, "3@cccc"],
      ]);

      const save = doc1.save();
      const doc4 = load(save);
      assert.deepEqual(doc4.save(), save);
    });

    it("paths can be used instead of objids", () => {
      const doc = create({ actor: "aaaa" });
      doc.putObject("_root", "list", [{ foo: "bar" }, [1, 2, 3]]);
      assert.deepEqual(doc.materialize("/"), {
        list: [{ foo: "bar" }, [1, 2, 3]],
      });
      assert.deepEqual(doc.materialize("/list"), [{ foo: "bar" }, [1, 2, 3]]);
      assert.deepEqual(doc.materialize("/list/0"), { foo: "bar" });
    });

    it("should be able to fetch changes by hash", () => {
      const doc1 = create({ actor: "aaaa" });
      const doc2 = create({ actor: "bbbb" });
      doc1.put("/", "a", "b");
      doc2.put("/", "b", "c");
      const head1 = doc1.getHeads();
      const head2 = doc2.getHeads();
      const change1 = doc1.getChangeByHash(head1[0]);
      const change2 = doc1.getChangeByHash(head2[0]);
      assert.deepEqual(change2, null);
      if (change1 === null) {
        throw new RangeError("change1 should not be null");
      }
      assert.deepEqual(decodeChange(change1).hash, head1[0]);
    });

    it("recursive sets are possible", () => {
      const doc = create({ actor: "aaaa" });
      const l1 = doc.putObject("_root", "list", [{ foo: "bar" }, [1, 2, 3]]);
      const l2 = doc.insertObject(l1, 0, { zip: ["a", "b"] });
      doc.putObject("_root", "info1", "hello world"); // 'text' object
      doc.put("_root", "info2", "hello world"); // 'str'
      const l4 = doc.putObject("_root", "info3", "hello world");
      assert.deepEqual(doc.materialize(), {
        list: [{ zip: ["a", "b"] }, { foo: "bar" }, [1, 2, 3]],
        info1: "hello world",
        info2: "hello world",
        info3: "hello world",
      });
      assert.deepEqual(doc.materialize(l2), { zip: ["a", "b"] });
      assert.deepEqual(doc.materialize(l1), [
        { zip: ["a", "b"] },
        { foo: "bar" },
        [1, 2, 3],
      ]);
      assert.deepEqual(doc.materialize(l4), "hello world");
    });

    it("only returns an object id when objects are created", () => {
      const doc = create({ actor: "aaaa" });
      const r1 = doc.put("_root", "foo", "bar");
      const r2 = doc.putObject("_root", "list", []);
      const r3 = doc.put("_root", "counter", 10, "counter");
      const r4 = doc.increment("_root", "counter", 1);
      const r5 = doc.delete("_root", "counter");
      const r6 = doc.insert(r2, 0, 10);
      const r7 = doc.insertObject(r2, 0, {});
      const r8 = doc.splice(r2, 1, 0, ["a", "b", "c"]);
      //let r9 = doc.splice(r2,1,0,["a",[],{},"d"]);
      assert.deepEqual(r1, null);
      assert.deepEqual(r2, "2@aaaa");
      assert.deepEqual(r3, null);
      assert.deepEqual(r4, null);
      assert.deepEqual(r5, null);
      assert.deepEqual(r6, null);
      assert.deepEqual(r7, "7@aaaa");
      assert.deepEqual(r8, null);
      //assert.deepEqual(r9,["12@aaaa","13@aaaa"]);
    });

    it("objects without properties are preserved", () => {
      const doc1 = create({ actor: "aaaa" });
      const a = doc1.putObject("_root", "a", {});
      const b = doc1.putObject("_root", "b", {});
      const c = doc1.putObject("_root", "c", {});
      doc1.put(c, "d", "dd");
      const saved = doc1.save();
      const doc2 = load(saved);
      assert.deepEqual(doc2.getWithType("_root", "a"), ["map", a]);
      assert.deepEqual(doc2.keys(a), []);
      assert.deepEqual(doc2.getWithType("_root", "b"), ["map", b]);
      assert.deepEqual(doc2.keys(b), []);
      assert.deepEqual(doc2.getWithType("_root", "c"), ["map", c]);
      assert.deepEqual(doc2.keys(c), ["d"]);
      assert.deepEqual(doc2.getWithType(c, "d"), ["str", "dd"]);
    });

    it("should allow you to fork at a heads", () => {
      const A = create({ actor: "aaaaaa" });
      A.put("/", "key1", "val1");
      A.put("/", "key2", "val2");
      const heads1 = A.getHeads();
      const B = A.fork("bbbbbb");
      A.put("/", "key3", "val3");
      B.put("/", "key4", "val4");
      A.merge(B);
      const heads2 = A.getHeads();
      A.put("/", "key5", "val5");
      assert.deepEqual(
        A.fork(undefined, heads1).materialize("/"),
        A.materialize("/", heads1),
      );
      assert.deepEqual(
        A.fork(undefined, heads2).materialize("/"),
        A.materialize("/", heads2),
      );
    });

    it("should handle merging text conflicts then saving & loading", () => {
      const A = create({ actor: "aabbcc" });
      const At = A.putObject("_root", "text", "");
      A.splice(At, 0, 0, "hello");

      const B = A.fork();

      assert.deepEqual(B.getWithType("_root", "text"), ["text", At]);

      B.splice(At, 4, 1);
      B.splice(At, 4, 0, "!");
      B.splice(At, 5, 0, " ");
      B.splice(At, 6, 0, "world");

      A.merge(B);

      const binary = A.save();

      const C = load(binary);

      assert.deepEqual(C.getWithType("_root", "text"), ["text", "1@aabbcc"]);
      assert.deepEqual(C.text(At), "hell! world");
    });
  });

  describe("loadIncremental", () => {
    it("should allow you to load changes with missing deps", () => {
      const doc1 = create({ actor: "aaaa" });
      doc1.put("_root", "key", "value");
      doc1.saveIncremental();
      doc1.put("_root", "key", "value2");
      const changeWithoutDep = doc1.saveIncremental();

      const doc2 = create({ actor: "bbbb" });
      doc2.loadIncremental(changeWithoutDep);
    });
  });

  describe("load", () => {
    it("should allow explicitly allowing missing deps", () => {
      const doc1 = create({ actor: "aaaa" });
      doc1.put("_root", "key", "value");
      doc1.saveIncremental();
      doc1.put("_root", "key", "value2");
      const changeWithoutDep = doc1.saveIncremental();

      load(changeWithoutDep, { allowMissingDeps: true });
    });
  });

  describe("patch generation", () => {
    it("should include root object key updates", () => {
      const doc1 = create({ actor: "aaaa" }),
        doc2 = create({ actor: "bbbb" });
      doc1.put("_root", "hello", "world");
      doc2.loadIncremental(doc1.saveIncremental());
      assert.deepEqual(doc2.diffIncremental(), [
        { action: "put", path: ["hello"], value: "world" },
      ]);
    });

    it("should include nested object creation", () => {
      const doc1 = create({ actor: "aaaa" }),
        doc2 = create({ actor: "bbbb" });
      doc1.putObject("_root", "birds", { friday: { robins: 3 } });
      doc2.loadIncremental(doc1.saveIncremental());
      assert.deepEqual(doc2.diffIncremental(), [
        { action: "put", path: ["birds"], value: {} },
        { action: "put", path: ["birds", "friday"], value: {} },
        { action: "put", path: ["birds", "friday", "robins"], value: 3 },
      ]);
    });

    it("should delete map keys", () => {
      const doc1 = create({ actor: "aaaa" }),
        doc2 = create({ actor: "bbbb" });
      doc1.put("_root", "favouriteBird", "Robin");
      doc2.loadIncremental(doc1.saveIncremental());
      doc1.delete("_root", "favouriteBird");
      doc2.loadIncremental(doc1.saveIncremental());
      assert.deepEqual(doc2.diffIncremental(), [
        //  { action: 'put', path: [ 'favouriteBird' ], value: 'Robin' },
        //  { action: 'del', path: [ 'favouriteBird' ] }
      ]);
    });

    it("should include list element insertion", () => {
      const doc1 = create({ actor: "aaaa" }),
        doc2 = create({ actor: "bbbb" });
      doc1.putObject("_root", "birds", ["Goldfinch", "Chaffinch"]);
      doc2.loadIncremental(doc1.saveIncremental());
      assert.deepEqual(doc2.diffIncremental(), [
        { action: "put", path: ["birds"], value: [] },
        {
          action: "insert",
          path: ["birds", 0],
          values: ["Goldfinch", "Chaffinch"],
        },
      ]);
    });

    it("should insert nested maps into a list", () => {
      const doc1 = create({ actor: "aaaa" }),
        doc2 = create({ actor: "bbbb" });
      doc1.putObject("_root", "birds", []);
      doc2.loadIncremental(doc1.saveIncremental());
      doc1.insertObject("1@aaaa", 0, { count: 3, species: "Goldfinch" });
      assert.deepEqual(doc2.diffIncremental(), [
        { action: "put", path: ["birds"], value: [] },
      ]);
      doc2.loadIncremental(doc1.saveIncremental());
      assert.deepEqual(doc2.diffIncremental(), [
        { action: "insert", path: ["birds", 0], values: [{}] },
        { action: "put", path: ["birds", 0, "count"], value: 3 },
        { action: "put", path: ["birds", 0, "species"], value: "Goldfinch" },
      ]);
    });

    it("should calculate list indexes based on visible elements", () => {
      const doc1 = create({ actor: "aaaa" }),
        doc2 = create({ actor: "bbbb" });
      doc1.putObject("_root", "birds", ["Goldfinch", "Chaffinch"]);
      doc2.loadIncremental(doc1.saveIncremental());
      assert.deepEqual(doc2.diffIncremental(), [
        { action: "put", path: ["birds"], value: [] },
        {
          action: "insert",
          path: ["birds", 0],
          values: ["Goldfinch", "Chaffinch"],
        },
      ]);
      doc1.delete("1@aaaa", 0);
      doc1.insert("1@aaaa", 1, "Greenfinch");
      doc2.loadIncremental(doc1.saveIncremental());
      assert.deepEqual(doc1.getWithType("1@aaaa", 0), ["str", "Chaffinch"]);
      assert.deepEqual(doc1.getWithType("1@aaaa", 1), ["str", "Greenfinch"]);
      assert.deepEqual(doc2.diffIncremental(), [
        { action: "del", path: ["birds", 0] },
        { action: "insert", path: ["birds", 1], values: ["Greenfinch"] },
      ]);
    });

    it("should handle concurrent insertions at the head of a list", () => {
      const doc1 = create({ actor: "aaaa" }),
        doc2 = create({ actor: "bbbb" }),
        doc3 = create({ actor: "cccc" }),
        doc4 = create({ actor: "dddd" });
      doc1.putObject("_root", "values", []);
      const change1 = doc1.saveIncremental();
      doc2.loadIncremental(change1);
      doc3.loadIncremental(change1);
      doc4.loadIncremental(change1);
      doc1.insert("1@aaaa", 0, "c");
      doc1.insert("1@aaaa", 1, "d");
      doc2.insert("1@aaaa", 0, "a");
      doc2.insert("1@aaaa", 1, "b");
      const change2 = doc1.saveIncremental(),
        change3 = doc2.saveIncremental();
      assert.deepEqual(doc3.diffIncremental(), [
        { action: "put", path: ["values"], value: [] },
      ]);
      assert.deepEqual(doc4.diffIncremental(), [
        { action: "put", path: ["values"], value: [] },
      ]);
      doc3.loadIncremental(change2);
      doc3.loadIncremental(change3);
      doc4.loadIncremental(change3);
      doc4.loadIncremental(change2);
      assert.deepEqual(
        [0, 1, 2, 3].map((i) => (doc3.getWithType("1@aaaa", i) || [])[1]),
        ["a", "b", "c", "d"],
      );
      assert.deepEqual(
        [0, 1, 2, 3].map((i) => (doc4.getWithType("1@aaaa", i) || [])[1]),
        ["a", "b", "c", "d"],
      );
      assert.deepEqual(doc3.diffIncremental(), [
        { action: "insert", path: ["values", 0], values: ["a", "b", "c", "d"] },
      ]);
      assert.deepEqual(doc4.diffIncremental(), [
        { action: "insert", path: ["values", 0], values: ["a", "b", "c", "d"] },
      ]);
    });

    it("should handle concurrent insertions beyond the head", () => {
      const doc1 = create({ actor: "aaaa" }),
        doc2 = create({ actor: "bbbb" }),
        doc3 = create({ actor: "cccc" }),
        doc4 = create({ actor: "dddd" });
      doc1.putObject("_root", "values", ["a", "b"]);
      const change1 = doc1.saveIncremental();
      doc2.loadIncremental(change1);
      doc3.loadIncremental(change1);
      doc4.loadIncremental(change1);
      doc1.insert("1@aaaa", 2, "e");
      doc1.insert("1@aaaa", 3, "f");
      doc2.insert("1@aaaa", 2, "c");
      doc2.insert("1@aaaa", 3, "d");
      const change2 = doc1.saveIncremental(),
        change3 = doc2.saveIncremental();
      assert.deepEqual(doc3.diffIncremental(), [
        { action: "put", path: ["values"], value: [] },
        { action: "insert", path: ["values", 0], values: ["a", "b"] },
      ]);
      assert.deepEqual(doc4.diffIncremental(), [
        { action: "put", path: ["values"], value: [] },
        { action: "insert", path: ["values", 0], values: ["a", "b"] },
      ]);
      doc3.loadIncremental(change2);
      doc3.loadIncremental(change3);
      doc4.loadIncremental(change3);
      doc4.loadIncremental(change2);
      assert.deepEqual(
        [0, 1, 2, 3, 4, 5].map((i) => (doc3.getWithType("1@aaaa", i) || [])[1]),
        ["a", "b", "c", "d", "e", "f"],
      );
      assert.deepEqual(
        [0, 1, 2, 3, 4, 5].map((i) => (doc4.getWithType("1@aaaa", i) || [])[1]),
        ["a", "b", "c", "d", "e", "f"],
      );
      assert.deepEqual(doc3.diffIncremental(), [
        { action: "insert", path: ["values", 2], values: ["c", "d", "e", "f"] },
      ]);
      assert.deepEqual(doc4.diffIncremental(), [
        { action: "insert", path: ["values", 2], values: ["c", "d", "e", "f"] },
      ]);
    });

    it("should handle conflicts on root object keys", () => {
      const doc1 = create({ actor: "aaaa" }),
        doc2 = create({ actor: "bbbb" }),
        doc3 = create({ actor: "cccc" }),
        doc4 = create({ actor: "dddd" });
      doc1.put("_root", "bird", "Greenfinch");
      doc2.put("_root", "bird", "Goldfinch");
      const change1 = doc1.saveIncremental(),
        change2 = doc2.saveIncremental();
      assert.deepEqual(doc3.diffIncremental(), doc4.diffIncremental());
      doc3.loadIncremental(change1);
      doc3.loadIncremental(change2);
      doc4.loadIncremental(change2);
      doc4.loadIncremental(change1);
      assert.deepEqual(doc3.getWithType("_root", "bird"), ["str", "Goldfinch"]);
      assert.deepEqual(doc3.getAll("_root", "bird"), [
        ["str", "Greenfinch", "1@aaaa"],
        ["str", "Goldfinch", "1@bbbb"],
      ]);
      assert.deepEqual(doc4.getWithType("_root", "bird"), ["str", "Goldfinch"]);
      assert.deepEqual(doc4.getAll("_root", "bird"), [
        ["str", "Greenfinch", "1@aaaa"],
        ["str", "Goldfinch", "1@bbbb"],
      ]);
      assert.deepEqual(doc3.diffIncremental(), [
        { action: "put", path: ["bird"], conflict: true, value: "Goldfinch" },
      ]);
      assert.deepEqual(doc4.diffIncremental(), [
        { action: "put", path: ["bird"], conflict: true, value: "Goldfinch" },
      ]);
    });

    it("should handle three-way conflicts", () => {
      const doc1 = create({ actor: "aaaa" }),
        doc2 = create({ actor: "bbbb" }),
        doc3 = create({ actor: "cccc" });
      doc1.put("_root", "bird", "Greenfinch");
      doc2.put("_root", "bird", "Chaffinch");
      doc3.put("_root", "bird", "Goldfinch");
      const change1 = doc1.saveIncremental(),
        change2 = doc2.saveIncremental(),
        change3 = doc3.saveIncremental();
      assert.deepEqual(doc1.diffIncremental(), [
        { action: "put", path: ["bird"], value: "Greenfinch" },
      ]);
      assert.deepEqual(doc2.diffIncremental(), [
        { action: "put", path: ["bird"], value: "Chaffinch" },
      ]);
      assert.deepEqual(doc3.diffIncremental(), [
        { action: "put", path: ["bird"], value: "Goldfinch" },
      ]);
      doc1.loadIncremental(change2);
      doc1.loadIncremental(change3);
      doc2.loadIncremental(change3);
      doc2.loadIncremental(change1);
      doc3.loadIncremental(change1);
      doc3.loadIncremental(change2);
      assert.deepEqual(doc1.getWithType("_root", "bird"), ["str", "Goldfinch"]);
      assert.deepEqual(doc1.getAll("_root", "bird"), [
        ["str", "Greenfinch", "1@aaaa"],
        ["str", "Chaffinch", "1@bbbb"],
        ["str", "Goldfinch", "1@cccc"],
      ]);
      assert.deepEqual(doc2.getWithType("_root", "bird"), ["str", "Goldfinch"]);
      assert.deepEqual(doc2.getAll("_root", "bird"), [
        ["str", "Greenfinch", "1@aaaa"],
        ["str", "Chaffinch", "1@bbbb"],
        ["str", "Goldfinch", "1@cccc"],
      ]);
      assert.deepEqual(doc3.getWithType("_root", "bird"), ["str", "Goldfinch"]);
      assert.deepEqual(doc3.getAll("_root", "bird"), [
        ["str", "Greenfinch", "1@aaaa"],
        ["str", "Chaffinch", "1@bbbb"],
        ["str", "Goldfinch", "1@cccc"],
      ]);
      assert.deepEqual(doc1.diffIncremental(), [
        { action: "put", path: ["bird"], conflict: true, value: "Chaffinch" },
        { action: "put", path: ["bird"], conflict: true, value: "Goldfinch" },
      ]);
      assert.deepEqual(doc2.diffIncremental(), [
        { action: "put", path: ["bird"], conflict: true, value: "Goldfinch" },
      ]);
      assert.deepEqual(doc3.diffIncremental(), [
        { action: "conflict", path: ["bird"] },
      ]);
    });

    it("should allow a conflict to be resolved", () => {
      const doc1 = create({ actor: "aaaa" }),
        doc2 = create({ actor: "bbbb" }),
        doc3 = create({ actor: "cccc" });
      // diffIncremental() from empty doc collapses conflicts
      // we dont want that for this test
      doc3.emptyChange();
      doc1.put("_root", "bird", "Greenfinch");
      doc2.put("_root", "bird", "Chaffinch");
      assert.deepEqual(doc3.diffIncremental(), []);
      const change1 = doc1.saveIncremental(),
        change2 = doc2.saveIncremental();
      doc1.loadIncremental(change2);
      doc3.loadIncremental(change1);
      doc2.loadIncremental(change1);
      doc3.loadIncremental(change2);
      doc1.put("_root", "bird", "Goldfinch");
      doc3.loadIncremental(doc1.saveIncremental());
      assert.deepEqual(doc3.getAll("_root", "bird"), [
        ["str", "Goldfinch", "2@aaaa"],
      ]);
      assert.deepEqual(doc3.diffIncremental(), [
        { action: "put", path: ["bird"], value: "Greenfinch" },
        { action: "put", path: ["bird"], value: "Chaffinch", conflict: true },
        { action: "put", path: ["bird"], value: "Goldfinch" },
      ]);
    });

    it("should handle a concurrent map key overwrite and delete", () => {
      const doc1 = create({ actor: "aaaa" }),
        doc2 = create({ actor: "bbbb" });
      doc1.put("_root", "bird", "Greenfinch");
      doc2.loadIncremental(doc1.saveIncremental());
      doc1.put("_root", "bird", "Goldfinch");
      doc2.delete("_root", "bird");
      const change1 = doc1.saveIncremental(),
        change2 = doc2.saveIncremental();
      assert.deepEqual(doc1.diffIncremental(), [
        { action: "put", path: ["bird"], value: "Goldfinch" },
      ]);
      assert.deepEqual(doc2.diffIncremental(), []);
      doc1.loadIncremental(change2);
      doc2.loadIncremental(change1);
      assert.deepEqual(doc1.getWithType("_root", "bird"), ["str", "Goldfinch"]);
      assert.deepEqual(doc1.getAll("_root", "bird"), [
        ["str", "Goldfinch", "2@aaaa"],
      ]);
      assert.deepEqual(doc2.getWithType("_root", "bird"), ["str", "Goldfinch"]);
      assert.deepEqual(doc2.getAll("_root", "bird"), [
        ["str", "Goldfinch", "2@aaaa"],
      ]);
      assert.deepEqual(doc1.diffIncremental(), []);
      assert.deepEqual(doc2.diffIncremental(), [
        { action: "put", path: ["bird"], value: "Goldfinch" },
      ]);
    });

    it("should handle a conflict on a list element", () => {
      const doc1 = create({ actor: "aaaa" }),
        doc2 = create({ actor: "bbbb" }),
        doc3 = create({ actor: "cccc" }),
        doc4 = create({ actor: "dddd" });
      doc1.putObject("_root", "birds", ["Thrush", "Magpie"]);
      const change1 = doc1.saveIncremental();
      doc2.loadIncremental(change1);
      doc3.loadIncremental(change1);
      doc4.loadIncremental(change1);
      doc1.put("1@aaaa", 0, "Song Thrush");
      doc2.put("1@aaaa", 0, "Redwing");
      const change2 = doc1.saveIncremental(),
        change3 = doc2.saveIncremental();
      assert.deepEqual(doc3.diffIncremental(), doc4.diffIncremental());
      doc3.loadIncremental(change2);
      doc3.loadIncremental(change3);
      doc4.loadIncremental(change3);
      doc4.loadIncremental(change2);
      assert.deepEqual(doc3.getWithType("1@aaaa", 0), ["str", "Redwing"]);
      assert.deepEqual(doc3.getAll("1@aaaa", 0), [
        ["str", "Song Thrush", "4@aaaa"],
        ["str", "Redwing", "4@bbbb"],
      ]);
      assert.deepEqual(doc4.getWithType("1@aaaa", 0), ["str", "Redwing"]);
      assert.deepEqual(doc4.getAll("1@aaaa", 0), [
        ["str", "Song Thrush", "4@aaaa"],
        ["str", "Redwing", "4@bbbb"],
      ]);
      assert.deepEqual(doc3.diffIncremental(), [
        { action: "put", path: ["birds", 0], value: "Song Thrush" },
        { action: "put", path: ["birds", 0], value: "Redwing", conflict: true },
      ]);
      assert.deepEqual(doc4.diffIncremental(), [
        { action: "put", path: ["birds", 0], value: "Redwing", conflict: true },
      ]);
    });

    it("should handle a concurrent list element overwrite and delete", () => {
      const doc1 = create({ actor: "aaaa" }),
        doc2 = create({ actor: "bbbb" }),
        doc3 = create({ actor: "cccc" }),
        doc4 = create({ actor: "dddd" });
      doc1.putObject("_root", "birds", ["Parakeet", "Magpie", "Thrush"]);
      const change1 = doc1.saveIncremental();
      doc2.loadIncremental(change1);
      doc3.loadIncremental(change1);
      doc4.loadIncremental(change1);
      doc1.delete("1@aaaa", 0);
      doc1.put("1@aaaa", 1, "Song Thrush");
      doc2.put("1@aaaa", 0, "Ring-necked parakeet");
      doc2.put("1@aaaa", 2, "Redwing");
      const change2 = doc1.saveIncremental(),
        change3 = doc2.saveIncremental();
      assert.deepEqual(doc3.diffIncremental(), doc4.diffIncremental());
      doc3.loadIncremental(change2);
      doc3.loadIncremental(change3);
      doc4.loadIncremental(change3);
      doc4.loadIncremental(change2);
      assert.deepEqual(doc3.getAll("1@aaaa", 0), [
        ["str", "Ring-necked parakeet", "5@bbbb"],
      ]);
      assert.deepEqual(doc3.getAll("1@aaaa", 2), [
        ["str", "Song Thrush", "6@aaaa"],
        ["str", "Redwing", "6@bbbb"],
      ]);
      assert.deepEqual(doc4.getAll("1@aaaa", 0), [
        ["str", "Ring-necked parakeet", "5@bbbb"],
      ]);
      assert.deepEqual(doc4.getAll("1@aaaa", 2), [
        ["str", "Song Thrush", "6@aaaa"],
        ["str", "Redwing", "6@bbbb"],
      ]);
      assert.deepEqual(doc3.diffIncremental(), [
        { action: "del", path: ["birds", 0] },
        { action: "put", path: ["birds", 1], value: "Song Thrush" },
        {
          action: "insert",
          path: ["birds", 0],
          values: ["Ring-necked parakeet"],
        },
        { action: "put", path: ["birds", 2], value: "Redwing", conflict: true },
      ]);
      assert.deepEqual(doc4.diffIncremental(), [
        { action: "put", path: ["birds", 0], value: "Ring-necked parakeet" },
        { action: "put", path: ["birds", 2], value: "Redwing", conflict: true },
      ]);
    });

    it("should handle deletion of a conflict value", () => {
      const doc1 = create({ actor: "aaaa" }),
        doc2 = create({ actor: "bbbb" }),
        doc3 = create({ actor: "cccc" });
      doc1.put("_root", "bird", "Robin");
      doc2.put("_root", "bird", "Wren");
      const change1 = doc1.saveIncremental(),
        change2 = doc2.saveIncremental();
      doc2.delete("_root", "bird");
      const change3 = doc2.saveIncremental();
      assert.deepEqual(doc3.diffIncremental(), []);
      doc3.loadIncremental(change1);
      doc3.loadIncremental(change2);
      assert.deepEqual(doc3.getAll("_root", "bird"), [
        ["str", "Robin", "1@aaaa"],
        ["str", "Wren", "1@bbbb"],
      ]);
      assert.deepEqual(doc3.diffIncremental(), [
        { action: "put", path: ["bird"], value: "Wren", conflict: true },
      ]);
      doc3.loadIncremental(change3);
      assert.deepEqual(doc3.getWithType("_root", "bird"), ["str", "Robin"]);
      assert.deepEqual(doc3.getAll("_root", "bird"), [
        ["str", "Robin", "1@aaaa"],
      ]);
      assert.deepEqual(doc3.diffIncremental(), [
        { action: "put", path: ["bird"], value: "Robin" },
      ]);
    });

    it("should handle conflicting nested objects", () => {
      const doc1 = create({ actor: "aaaa" }),
        doc2 = create({ actor: "bbbb" });
      doc1.putObject("_root", "birds", ["Parakeet"]);
      doc2.putObject("_root", "birds", { Sparrowhawk: 1 });
      const change1 = doc1.saveIncremental(),
        change2 = doc2.saveIncremental();
      assert.deepEqual(doc1.diffIncremental(), [
        { action: "put", path: ["birds"], value: [] },
        { action: "insert", path: ["birds", 0], values: ["Parakeet"] },
      ]);
      assert.deepEqual(doc2.diffIncremental(), [
        { action: "put", path: ["birds"], value: {} },
        { action: "put", path: ["birds", "Sparrowhawk"], value: 1 },
      ]);
      doc1.loadIncremental(change2);
      doc2.loadIncremental(change1);
      assert.deepEqual(doc1.getAll("_root", "birds"), [
        ["list", "1@aaaa"],
        ["map", "1@bbbb"],
      ]);
      assert.deepEqual(doc1.diffIncremental(), [
        { action: "put", path: ["birds"], value: {}, conflict: true },
        { action: "put", path: ["birds", "Sparrowhawk"], value: 1 },
      ]);
      assert.deepEqual(doc2.getAll("_root", "birds"), [
        ["list", "1@aaaa"],
        ["map", "1@bbbb"],
      ]);
      assert.deepEqual(doc2.diffIncremental(), [
        { action: "conflict", path: ["birds"] },
      ]);
    });

    it("should support date objects", () => {
      const doc1 = create({ actor: "aaaa" }),
        doc2 = create({ actor: "bbbb" }),
        now = new Date();
      doc1.put("_root", "createdAt", now);
      doc2.loadIncremental(doc1.saveIncremental());
      assert.deepEqual(doc2.getWithType("_root", "createdAt"), [
        "timestamp",
        now,
      ]);
      assert.deepEqual(doc2.diffIncremental(), [
        { action: "put", path: ["createdAt"], value: now },
      ]);
    });

    it("should capture local put ops", () => {
      const doc1 = create({ actor: "aaaa" });
      assert.deepEqual(doc1.diffIncremental(), []);
      doc1.put("_root", "key1", 1);
      doc1.put("_root", "key1", 2);
      doc1.put("_root", "key2", 3);
      doc1.putObject("_root", "map", {});
      doc1.putObject("_root", "list", []);

      assert.deepEqual(doc1.diffIncremental(), [
        { action: "put", path: ["key1"], value: 2 },
        { action: "put", path: ["key2"], value: 3 },
        { action: "put", path: ["list"], value: [] },
        { action: "put", path: ["map"], value: {} },
      ]);
    });

    it("should capture local insert ops", () => {
      const doc1 = create({ actor: "aaaa" });
      const list = doc1.putObject("_root", "list", []);
      doc1.insert(list, 0, 1);
      doc1.insert(list, 0, 2);
      doc1.insert(list, 2, 3);
      doc1.insertObject(list, 2, {});
      doc1.insertObject(list, 2, []);

      assert.deepEqual(doc1.diffIncremental(), [
        { action: "put", path: ["list"], value: [] },
        { action: "insert", path: ["list", 0], values: [2, 1, [], {}, 3] },
      ]);
    });

    it("should capture local push ops", () => {
      const doc1 = create({ actor: "aaaa" });
      const list = doc1.putObject("_root", "list", []);
      doc1.push(list, 1);
      doc1.pushObject(list, {});
      doc1.pushObject(list, []);

      assert.deepEqual(doc1.diffIncremental(), [
        { action: "put", path: ["list"], value: [] },
        { action: "insert", path: ["list", 0], values: [1, {}, []] },
      ]);
    });

    it("should capture local splice ops", () => {
      const doc1 = create({ actor: "aaaa" });
      const list = doc1.putObject("_root", "list", []);
      doc1.splice(list, 0, 0, [1, 2, 3, 4]);
      doc1.splice(list, 1, 2);

      assert.deepEqual(doc1.diffIncremental(), [
        { action: "put", path: ["list"], value: [] },
        { action: "insert", path: ["list", 0], values: [1, 4] },
      ]);
    });

    it("should capture local increment ops", () => {
      const doc1 = create({ actor: "aaaa" });
      // the first diff incremental collapses increments for efficent loading
      doc1.put("_root", "counter0", 10, "counter");
      doc1.increment("_root", "counter0", 2);
      assert.deepEqual(doc1.diffIncremental(), [
        { action: "put", path: ["counter0"], value: 12 },
      ]);
      assert.deepEqual(doc1.diffIncremental(), []);
      doc1.put("_root", "counter", 2, "counter");
      doc1.increment("_root", "counter", 4);

      assert.deepEqual(doc1.diffIncremental(), [
        { action: "put", path: ["counter"], value: 2 },
        { action: "inc", path: ["counter"], value: 4 },
      ]);
    });

    it("should capture local delete ops", () => {
      const doc1 = create({ actor: "aaaa" });
      // the first diff incremental collapses deletes for efficient loading
      doc1.put("_root", "key0", 1);
      doc1.delete("_root", "key0");
      assert.deepEqual(doc1.diffIncremental(), []);
      doc1.put("_root", "key1", 1);
      doc1.put("_root", "key2", 2);
      doc1.delete("_root", "key1");
      doc1.delete("_root", "key2");
      assert.deepEqual(doc1.diffIncremental(), [
        { action: "put", path: ["key1"], value: 1 },
        { action: "put", path: ["key2"], value: 2 },
        { action: "del", path: ["key1"] },
        { action: "del", path: ["key2"] },
      ]);
    });

    it("should support counters in a map", () => {
      const doc1 = create({ actor: "aaaa" }),
        doc2 = create({ actor: "bbbb" });
      assert.deepEqual(doc2.diffIncremental(), []);
      doc1.put("_root", "starlings", 2, "counter");
      doc2.loadIncremental(doc1.saveIncremental());
      doc1.increment("_root", "starlings", 1);
      doc2.loadIncremental(doc1.saveIncremental());
      assert.deepEqual(doc2.getWithType("_root", "starlings"), ["counter", 3]);
      assert.deepEqual(doc2.diffIncremental(), [
        { action: "put", path: ["starlings"], value: 3 },
      ]);
    });

    it("should support counters in a list", () => {
      const doc1 = create({ actor: "aaaa" }),
        doc2 = create({ actor: "bbbb" });
      assert.deepEqual(doc2.diffIncremental(), []);
      const list = doc1.putObject("_root", "list", []);
      doc2.loadIncremental(doc1.saveIncremental());
      doc1.insert(list, 0, 1, "counter");
      doc2.loadIncremental(doc1.saveIncremental());
      doc1.increment(list, 0, 2);
      doc2.loadIncremental(doc1.saveIncremental());
      doc1.increment(list, 0, -5);
      doc2.loadIncremental(doc1.saveIncremental());

      assert.deepEqual(doc2.diffIncremental(), [
        { action: "put", path: ["list"], value: [] },
        { action: "insert", path: ["list", 0], values: [-2] },
      ]);
    });

    it("should delete a counter from a map"); // TODO
  });

  describe("sync", () => {
    it("should send a sync message implying no local data", () => {
      const doc = create();
      const s1 = initSyncState();
      const m1 = doc.generateSyncMessage(s1);
      if (m1 === null) {
        throw new RangeError("message should not be null");
      }
      const message: DecodedSyncMessage = decodeSyncMessage(m1);
      assert.deepStrictEqual(message.heads, []);
      assert.deepStrictEqual(message.need, []);
      assert.deepStrictEqual(message.have.length, 1);
      assert.deepStrictEqual(message.have[0].lastSync, []);
      assert.deepStrictEqual(message.have[0].bloom.byteLength, 0);
      assert.deepStrictEqual(message.changes, []);
    });

    it("should not reply if we have no data as well after the first round", () => {
      const n1 = create(),
        n2 = create();
      const s1 = initSyncState(),
        s2 = initSyncState();
      let m1 = n1.generateSyncMessage(s1);
      if (m1 === null) {
        throw new RangeError("message should not be null");
      }
      n2.receiveSyncMessage(s2, m1);
      let m2 = n2.generateSyncMessage(s2);
      // We should always send a message on the first round to advertise our heads
      assert.notStrictEqual(m2, null);
      n2.receiveSyncMessage(s2, m2!);

      // now make a change on n1 so we generate another sync message to send
      n1.put("_root", "x", 1);
      m1 = n1.generateSyncMessage(s1);
      n2.receiveSyncMessage(s2, m2!);

      m2 = n2.generateSyncMessage(s2);
      assert.deepStrictEqual(m2, null);
    });

    it("repos with equal heads do not need a reply message after the first round", () => {
      const n1 = create(),
        n2 = create();
      const s1 = initSyncState(),
        s2 = initSyncState();

      // make two nodes with the same changes
      const list = n1.putObject("_root", "n", []);
      n1.commit("", 0);
      for (let i = 0; i < 10; i++) {
        n1.insert(list, i, i);
        n1.commit("", 0);
      }
      n2.applyChanges(n1.getChanges([]));
      assert.deepStrictEqual(n1.materialize(), n2.materialize());

      // generate a naive sync message
      let m1 = n1.generateSyncMessage(s1);
      if (m1 === null) {
        throw new RangeError("message should not be null");
      }
      assert.deepStrictEqual(s1.lastSentHeads, n1.getHeads());

      // process the first response (which is always generated so we know the other ends heads)
      n2.receiveSyncMessage(s2, m1);
      const m2 = n2.generateSyncMessage(s2);
      n1.receiveSyncMessage(s1, m2!);

      // heads are equal so this message should be null
      m1 = n1.generateSyncMessage(s2);
      assert.strictEqual(m1, null);
    });

    it("n1 should offer all changes to n2 when starting from nothing", () => {
      const n1 = create(),
        n2 = create();

      // make changes for n1 that n2 should request
      const list = n1.putObject("_root", "n", []);
      n1.commit("", 0);
      for (let i = 0; i < 10; i++) {
        n1.insert(list, i, i);
        n1.commit("", 0);
      }

      assert.notDeepStrictEqual(n1.materialize(), n2.materialize());
      sync(n1, n2);
      assert.deepStrictEqual(n1.materialize(), n2.materialize());
    });

    it("should sync peers where one has commits the other does not", () => {
      const n1 = create(),
        n2 = create();

      // make changes for n1 that n2 should request
      const list = n1.putObject("_root", "n", []);
      n1.commit("", 0);
      for (let i = 0; i < 10; i++) {
        n1.insert(list, i, i);
        n1.commit("", 0);
      }

      assert.notDeepStrictEqual(n1.materialize(), n2.materialize());
      sync(n1, n2);
      assert.deepStrictEqual(n1.materialize(), n2.materialize());
    });

    it("should work with prior sync state", () => {
      // create & synchronize two nodes
      const n1 = create(),
        n2 = create();
      const s1 = initSyncState(),
        s2 = initSyncState();

      for (let i = 0; i < 5; i++) {
        n1.put("_root", "x", i);
        n1.commit("", 0);
      }

      sync(n1, n2, s1, s2);

      // modify the first node further
      for (let i = 5; i < 10; i++) {
        n1.put("_root", "x", i);
        n1.commit("", 0);
      }

      assert.notDeepStrictEqual(n1.materialize(), n2.materialize());
      sync(n1, n2, s1, s2);
      assert.deepStrictEqual(n1.materialize(), n2.materialize());
    });

    it("should not generate messages once synced", () => {
      // create & synchronize two nodes
      const n1 = create({ actor: "abc123" }),
        n2 = create({ actor: "def456" });
      const s1 = initSyncState(),
        s2 = initSyncState();

      let message;
      for (let i = 0; i < 5; i++) {
        n1.put("_root", "x", i);
        n1.commit("", 0);
      }
      for (let i = 0; i < 5; i++) {
        n2.put("_root", "y", i);
        n2.commit("", 0);
      }

      // n1 reports what it has
      message = n1.generateSyncMessage(s1);
      if (message === null) {
        throw new RangeError("message should not be null");
      }

      // n2 receives that message and sends changes along with what it has
      n2.receiveSyncMessage(s2, message);
      message = n2.generateSyncMessage(s2);
      if (message === null) {
        throw new RangeError("message should not be null");
      }
      assert(decodeSyncMessage(message).changes.length > 0);
      //assert.deepStrictEqual(patch, null) // no changes arrived

      // n1 receives the changes and replies with the changes it now knows that n2 needs
      n1.receiveSyncMessage(s1, message);
      message = n1.generateSyncMessage(s1);
      if (message === null) {
        throw new RangeError("message should not be null");
      }
      assert(decodeSyncMessage(message).changes.length > 0);

      // n2 applies the changes and sends confirmation ending the exchange
      n2.receiveSyncMessage(s2, message);
      message = n2.generateSyncMessage(s2);
      if (message === null) {
        throw new RangeError("message should not be null");
      }

      // n1 receives the message and has nothing more to say
      n1.receiveSyncMessage(s1, message);
      message = n1.generateSyncMessage(s1);
      assert.deepStrictEqual(message, null);
      //assert.deepStrictEqual(patch, null) // no changes arrived

      // n2 also has nothing left to say
      message = n2.generateSyncMessage(s2);
      assert.deepStrictEqual(message, null);
    });

    it("should allow simultaneous messages during synchronization", () => {
      // create & synchronize two nodes
      const n1 = create({ actor: "abc123" }),
        n2 = create({ actor: "def456" });
      const s1 = initSyncState(),
        s2 = initSyncState();

      for (let i = 0; i < 5; i++) {
        n1.put("_root", "x", i);
        n1.commit("", 0);
      }
      for (let i = 0; i < 5; i++) {
        n2.put("_root", "y", i);
        n2.commit("", 0);
      }

      const head1 = n1.getHeads()[0],
        head2 = n2.getHeads()[0];

      // both sides report what they have but have no shared peer state
      let msg1to2, msg2to1;
      msg1to2 = n1.generateSyncMessage(s1);
      msg2to1 = n2.generateSyncMessage(s2);
      if (msg1to2 === null) {
        throw new RangeError("message should not be null");
      }
      if (msg2to1 === null) {
        throw new RangeError("message should not be null");
      }
      assert.deepStrictEqual(decodeSyncMessage(msg1to2).changes.length, 0);
      assert.deepStrictEqual(
        decodeSyncMessage(msg1to2).have[0].lastSync.length,
        0,
      );
      assert.deepStrictEqual(decodeSyncMessage(msg2to1).changes.length, 0);
      assert.deepStrictEqual(
        decodeSyncMessage(msg2to1).have[0].lastSync.length,
        0,
      );

      // n1 and n2 receive that message and update sync state but make no patch
      n1.receiveSyncMessage(s1, msg2to1);
      n2.receiveSyncMessage(s2, msg1to2);

      // now both reply with their local changes the other lacks
      // (standard warning that 1% of the time this will result in a "need" message)
      msg1to2 = n1.generateSyncMessage(s1);
      if (msg1to2 === null) {
        throw new RangeError("message should not be null");
      }
      assert(decodeSyncMessage(msg1to2).changes.length > 0);
      msg2to1 = n2.generateSyncMessage(s2);
      if (msg2to1 === null) {
        throw new RangeError("message should not be null");
      }
      assert(decodeSyncMessage(msg2to1).changes.length > 0);

      // both should now apply the changes and update the frontend
      n1.receiveSyncMessage(s1, msg2to1);
      assert.deepStrictEqual(n1.getMissingDeps(), []);
      //assert.notDeepStrictEqual(patch1, null)
      assert.deepStrictEqual(n1.materialize(), { x: 4, y: 4 });

      n2.receiveSyncMessage(s2, msg1to2);
      assert.deepStrictEqual(n2.getMissingDeps(), []);
      //assert.notDeepStrictEqual(patch2, null)
      assert.deepStrictEqual(n2.materialize(), { x: 4, y: 4 });

      // The response acknowledges the changes received and sends no further changes
      msg1to2 = n1.generateSyncMessage(s1);
      if (msg1to2 === null) {
        throw new RangeError("message should not be null");
      }
      assert.deepStrictEqual(decodeSyncMessage(msg1to2).changes.length, 0);
      msg2to1 = n2.generateSyncMessage(s2);
      if (msg2to1 === null) {
        throw new RangeError("message should not be null");
      }
      assert.deepStrictEqual(decodeSyncMessage(msg2to1).changes.length, 0);

      // After receiving acknowledgements, their shared heads should be equal
      n1.receiveSyncMessage(s1, msg2to1);
      n2.receiveSyncMessage(s2, msg1to2);
      assert.deepStrictEqual(s1.sharedHeads, [head1, head2].sort());
      assert.deepStrictEqual(s2.sharedHeads, [head1, head2].sort());
      //assert.deepStrictEqual(patch1, null)
      //assert.deepStrictEqual(patch2, null)

      // We're in sync, no more messages required
      msg1to2 = n1.generateSyncMessage(s1);
      msg2to1 = n2.generateSyncMessage(s2);
      assert.deepStrictEqual(msg1to2, null);
      assert.deepStrictEqual(msg2to1, null);

      // If we make one more change and start another sync then its lastSync should be updated
      n1.put("_root", "x", 5);
      msg1to2 = n1.generateSyncMessage(s1);
      if (msg1to2 === null) {
        throw new RangeError("message should not be null");
      }
      assert.deepStrictEqual(
        decodeSyncMessage(msg1to2).have[0].lastSync,
        [head1, head2].sort(),
      );
    });

    it("should assume sent changes were received until we hear otherwise", () => {
      const n1 = create({ actor: "01234567" }),
        n2 = create({ actor: "89abcdef" });
      const s1 = initSyncState(),
        s2 = initSyncState();
      let message = null;

      const items = n1.putObject("_root", "items", []);
      n1.commit("", 0);

      sync(n1, n2, s1, s2);

      n1.push(items, "x");
      n1.commit("", 0);
      message = n1.generateSyncMessage(s1);
      if (message === null) {
        throw new RangeError("message should not be null");
      }
      assert.deepStrictEqual(decodeSyncMessage(message).changes.length, 1);

      n1.push(items, "y");
      n1.commit("", 0);
      message = n1.generateSyncMessage(s1);
      if (message === null) {
        throw new RangeError("message should not be null");
      }
      assert.deepStrictEqual(decodeSyncMessage(message).changes.length, 1);

      n1.push(items, "z");
      n1.commit("", 0);

      message = n1.generateSyncMessage(s1);
      if (message === null) {
        throw new RangeError("message should not be null");
      }
      assert.deepStrictEqual(decodeSyncMessage(message).changes.length, 1);
    });

    it("should work regardless of who initiates the exchange", () => {
      // create & synchronize two nodes
      const n1 = create(),
        n2 = create();
      const s1 = initSyncState(),
        s2 = initSyncState();

      for (let i = 0; i < 5; i++) {
        n1.put("_root", "x", i);
        n1.commit("", 0);
      }

      sync(n1, n2, s1, s2);

      // modify the first node further
      for (let i = 5; i < 10; i++) {
        n1.put("_root", "x", i);
        n1.commit("", 0);
      }

      assert.notDeepStrictEqual(n1.materialize(), n2.materialize());
      sync(n1, n2, s1, s2);
      assert.deepStrictEqual(n1.materialize(), n2.materialize());
    });

    it("should work without prior sync state", () => {
      // Scenario:                                                            ,-- c10 <-- c11 <-- c12 <-- c13 <-- c14
      // c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8 <-- c9 <-+
      //                                                                      `-- c15 <-- c16 <-- c17
      // lastSync is undefined.

      // create two peers both with divergent commits
      const n1 = create({ actor: "01234567" }),
        n2 = create({ actor: "89abcdef" });
      //const s1 = initSyncState(), s2 = initSyncState()

      for (let i = 0; i < 10; i++) {
        n1.put("_root", "x", i);
        n1.commit("", 0);
      }

      sync(n1, n2);

      for (let i = 10; i < 15; i++) {
        n1.put("_root", "x", i);
        n1.commit("", 0);
      }

      for (let i = 15; i < 18; i++) {
        n2.put("_root", "x", i);
        n2.commit("", 0);
      }

      assert.notDeepStrictEqual(n1.materialize(), n2.materialize());
      sync(n1, n2);
      assert.deepStrictEqual(n1.getHeads(), n2.getHeads());
      assert.deepStrictEqual(n1.materialize(), n2.materialize());
    });

    it("should work with prior sync state", () => {
      // Scenario:                                                            ,-- c10 <-- c11 <-- c12 <-- c13 <-- c14
      // c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8 <-- c9 <-+
      //                                                                      `-- c15 <-- c16 <-- c17
      // lastSync is c9.

      // create two peers both with divergent commits
      const n1 = create({ actor: "01234567" }),
        n2 = create({ actor: "89abcdef" });
      let s1 = initSyncState(),
        s2 = initSyncState();

      for (let i = 0; i < 10; i++) {
        n1.put("_root", "x", i);
        n1.commit("", 0);
      }

      sync(n1, n2, s1, s2);

      for (let i = 10; i < 15; i++) {
        n1.put("_root", "x", i);
        n1.commit("", 0);
      }
      for (let i = 15; i < 18; i++) {
        n2.put("_root", "x", i);
        n2.commit("", 0);
      }

      s1 = decodeSyncState(encodeSyncState(s1));
      s2 = decodeSyncState(encodeSyncState(s2));

      assert.notDeepStrictEqual(n1.materialize(), n2.materialize());
      sync(n1, n2, s1, s2);
      assert.deepStrictEqual(n1.getHeads(), n2.getHeads());
      assert.deepStrictEqual(n1.materialize(), n2.materialize());
    });

    it("should ensure non-empty state after sync", () => {
      const n1 = create({ actor: "01234567" }),
        n2 = create({ actor: "89abcdef" });
      const s1 = initSyncState(),
        s2 = initSyncState();

      for (let i = 0; i < 3; i++) {
        n1.put("_root", "x", i);
        n1.commit("", 0);
      }

      sync(n1, n2, s1, s2);

      assert.deepStrictEqual(s1.sharedHeads, n1.getHeads());
      assert.deepStrictEqual(s2.sharedHeads, n1.getHeads());
    });

    it("should re-sync after one node crashed with data loss", () => {
      // Scenario:     (r)                  (n2)                 (n1)
      // c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8
      // n2 has changes {c0, c1, c2}, n1's lastSync is c5, and n2's lastSync is c2.
      // we want to successfully sync (n1) with (r), even though (n1) believes it's talking to (n2)
      const n1 = create({ actor: "01234567" }),
        n2 = create({ actor: "89abcdef" });
      let s1 = initSyncState();
      const s2 = initSyncState();

      // n1 makes three changes, which we sync to n2
      for (let i = 0; i < 3; i++) {
        n1.put("_root", "x", i);
        n1.commit("", 0);
      }

      sync(n1, n2, s1, s2);

      // save a copy of n2 as "r" to simulate recovering from a crash
      let r;
      let rSyncState;
      [r, rSyncState] = [n2.clone(), s2.clone()];

      // sync another few commits
      for (let i = 3; i < 6; i++) {
        n1.put("_root", "x", i);
        n1.commit("", 0);
      }

      sync(n1, n2, s1, s2);

      // everyone should be on the same page here
      assert.deepStrictEqual(n1.getHeads(), n2.getHeads());
      assert.deepStrictEqual(n1.materialize(), n2.materialize());

      // now make a few more changes and then attempt to sync the fully-up-to-date n1 with the confused r
      for (let i = 6; i < 9; i++) {
        n1.put("_root", "x", i);
        n1.commit("", 0);
      }

      s1 = decodeSyncState(encodeSyncState(s1));
      rSyncState = decodeSyncState(encodeSyncState(rSyncState));

      assert.notDeepStrictEqual(n1.getHeads(), r.getHeads());
      assert.notDeepStrictEqual(n1.materialize(), r.materialize());
      assert.deepStrictEqual(n1.materialize(), { x: 8 });
      assert.deepStrictEqual(r.materialize(), { x: 2 });
      sync(n1, r, s1, rSyncState);
      assert.deepStrictEqual(n1.getHeads(), r.getHeads());
      assert.deepStrictEqual(n1.materialize(), r.materialize());
      r = null;
    });

    it("should re-sync after one node experiences data loss without disconnecting", () => {
      const n1 = create({ actor: "01234567" }),
        n2 = create({ actor: "89abcdef" });
      const s1 = initSyncState(),
        s2 = initSyncState();

      // n1 makes three changes, which we sync to n2
      for (let i = 0; i < 3; i++) {
        n1.put("_root", "x", i);
        n1.commit("", 0);
      }

      sync(n1, n2, s1, s2);

      assert.deepStrictEqual(n1.getHeads(), n2.getHeads());
      assert.deepStrictEqual(n1.materialize(), n2.materialize());

      const n2AfterDataLoss = create({ actor: "89abcdef" });

      // "n2" now has no data, but n1 still thinks it does. Note we don't do
      // decodeSyncState(encodeSyncState(s1)) in order to simulate data loss without disconnecting
      sync(n1, n2AfterDataLoss, s1, initSyncState());
      assert.deepStrictEqual(n1.getHeads(), n2.getHeads());
      assert.deepStrictEqual(n1.materialize(), n2.materialize());
    });

    it("should handle changes concurrent to the last sync heads", () => {
      const n1 = create({ actor: "01234567" }),
        n2 = create({ actor: "89abcdef" }),
        n3 = create({ actor: "fedcba98" });
      const s12 = initSyncState(),
        s21 = initSyncState(),
        s23 = initSyncState(),
        s32 = initSyncState();

      // Change 1 is known to all three nodes
      //n1 = Automerge.change(n1, {time: 0}, doc => doc.x = 1)
      n1.put("_root", "x", 1);
      n1.commit("", 0);

      sync(n1, n2, s12, s21);
      sync(n2, n3, s23, s32);

      // Change 2 is known to n1 and n2
      n1.put("_root", "x", 2);
      n1.commit("", 0);

      sync(n1, n2, s12, s21);

      // Each of the three nodes makes one change (changes 3, 4, 5)
      n1.put("_root", "x", 3);
      n1.commit("", 0);
      n2.put("_root", "x", 4);
      n2.commit("", 0);
      n3.put("_root", "x", 5);
      n3.commit("", 0);

      // Apply n3's latest change to n2. If running in Node, turn the Uint8Array into a Buffer, to
      // simulate transmission over a network (see https://github.com/automerge/automerge/pull/362)
      let change = n3.getLastLocalChange();
      if (change === null) throw new RangeError("no local change");
      //ts-ignore
      if (typeof Buffer === "function") change = Buffer.from(change);
      if (change === undefined) {
        throw new RangeError("last local change failed");
      }
      n2.applyChanges([change]);

      // Now sync n1 and n2. n3's change is concurrent to n1 and n2's last sync heads
      sync(n1, n2, s12, s21);
      assert.deepStrictEqual(n1.getHeads(), n2.getHeads());
      assert.deepStrictEqual(n1.materialize(), n2.materialize());
    });

    it("should handle histories with lots of branching and merging", () => {
      const n1 = create({ actor: "01234567" }),
        n2 = create({ actor: "89abcdef" }),
        n3 = create({ actor: "fedcba98" });
      n1.put("_root", "x", 0);
      n1.commit("", 0);
      const change1 = n1.getLastLocalChange();
      if (change1 === null) throw new RangeError("no local change");
      n2.applyChanges([change1]);
      const change2 = n1.getLastLocalChange();
      if (change2 === null) throw new RangeError("no local change");
      n3.applyChanges([change2]);
      n3.put("_root", "x", 1);
      n3.commit("", 0);

      //        - n1c1 <------ n1c2 <------ n1c3 <-- etc. <-- n1c20 <------ n1c21
      //       /          \/           \/                              \/
      //      /           /\           /\                              /\
      // c0 <---- n2c1 <------ n2c2 <------ n2c3 <-- etc. <-- n2c20 <------ n2c21
      //      \                                                          /
      //       ---------------------------------------------- n3c1 <-----
      for (let i = 1; i < 20; i++) {
        n1.put("_root", "n1", i);
        n1.commit("", 0);
        n2.put("_root", "n2", i);
        n2.commit("", 0);
        const change1 = n1.getLastLocalChange();
        if (change1 === null) throw new RangeError("no local change");
        const change2 = n2.getLastLocalChange();
        if (change2 === null) throw new RangeError("no local change");
        n1.applyChanges([change2]);
        n2.applyChanges([change1]);
      }

      const s1 = initSyncState(),
        s2 = initSyncState();
      sync(n1, n2, s1, s2);

      // Having n3's last change concurrent to the last sync heads forces us into the slower code path
      const change3 = n3.getLastLocalChange();
      if (change3 === null) throw new RangeError("no local change");
      n2.applyChanges([change3]);
      n1.put("_root", "n1", "final");
      n1.commit("", 0);
      n2.put("_root", "n2", "final");
      n2.commit("", 0);

      sync(n1, n2, s1, s2);
      assert.deepStrictEqual(n1.getHeads(), n2.getHeads());
      assert.deepStrictEqual(n1.materialize(), n2.materialize());
    });

    it("should handle a false-positive head", () => {
      // Scenario:                                                            ,-- n1
      // c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8 <-- c9 <-+
      //                                                                      `-- n2
      // where n2 is a false positive in the Bloom filter containing {n1}.
      // lastSync is c9.
      let n1 = create({ actor: "01234567" }),
        n2 = create({ actor: "89abcdef" });
      let s1 = initSyncState(),
        s2 = initSyncState();

      for (let i = 0; i < 10; i++) {
        n1.put("_root", "x", i);
        n1.commit("", 0);
      }

      sync(n1, n2, s1, s2);
      for (let i = 1; ; i++) {
        // search for false positive; see comment above
        const n1up = n1.clone("01234567");
        n1up.put("_root", "x", `${i} @ n1`);
        n1up.commit("", 0);

        const n2up = n2.clone("89abcdef");
        n2up.put("_root", "x", `${i} @ n2`);
        n2up.commit("", 0);
        const falsePositive = new BloomFilter(n1up.getHeads()).containsHash(
          n2up.getHeads()[0],
        );
        if (falsePositive) {
          n1 = n1up;
          n2 = n2up;
          break;
        }
      }
      const allHeads = [...n1.getHeads(), ...n2.getHeads()].sort();
      s1 = decodeSyncState(encodeSyncState(s1));
      s2 = decodeSyncState(encodeSyncState(s2));
      sync(n1, n2, s1, s2);
      assert.deepStrictEqual(n1.getHeads(), allHeads);
      assert.deepStrictEqual(n2.getHeads(), allHeads);
    });

    describe("with a false-positive dependency", () => {
      let n1: Automerge,
        n2: Automerge,
        s1: SyncState,
        s2: SyncState,
        n1hash2: Hash,
        n2hash2: Hash;

      beforeEach(() => {
        // Scenario:                                                            ,-- n1c1 <-- n1c2
        // c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8 <-- c9 <-+
        //                                                                      `-- n2c1 <-- n2c2
        // where n2c1 is a false positive in the Bloom filter containing {n1c1, n1c2}.
        // lastSync is c9.
        n1 = create({ actor: "01234567" });
        n2 = create({ actor: "89abcdef" });
        s1 = initSyncState();
        s2 = initSyncState();
        for (let i = 0; i < 10; i++) {
          n1.put("_root", "x", i);
          n1.commit("", 0);
        }
        sync(n1, n2, s1, s2);

        let n1hash1, n2hash1;
        for (let i = 29; ; i++) {
          // search for false positive; see comment above
          const n1us1 = n1.clone("01234567");
          n1us1.put("_root", "x", `${i} @ n1`);
          n1us1.commit("", 0);

          const n2us1 = n2.clone("89abcdef");
          n2us1.put("_root", "x", `${i} @ n1`);
          n2us1.commit("", 0);

          n1hash1 = n1us1.getHeads()[0];
          n2hash1 = n2us1.getHeads()[0];

          const n1us2 = n1us1.clone();
          n1us2.put("_root", "x", `final @ n1`);
          n1us2.commit("", 0);

          const n2us2 = n2us1.clone();
          n2us2.put("_root", "x", `final @ n2`);
          n2us2.commit("", 0);

          n1hash2 = n1us2.getHeads()[0];
          n2hash2 = n2us2.getHeads()[0];
          if (new BloomFilter([n1hash1, n1hash2]).containsHash(n2hash1)) {
            n1 = n1us2;
            n2 = n2us2;
            break;
          }
        }
      });

      it("should sync two nodes without connection reset", () => {
        sync(n1, n2, s1, s2);
        assert.deepStrictEqual(n1.getHeads(), [n1hash2, n2hash2].sort());
        assert.deepStrictEqual(n2.getHeads(), [n1hash2, n2hash2].sort());
      });

      it("should sync two nodes with connection reset", () => {
        s1 = decodeSyncState(encodeSyncState(s1));
        s2 = decodeSyncState(encodeSyncState(s2));
        sync(n1, n2, s1, s2);
        assert.deepStrictEqual(n1.getHeads(), [n1hash2, n2hash2].sort());
        assert.deepStrictEqual(n2.getHeads(), [n1hash2, n2hash2].sort());
      });

      it("should sync three nodes", () => {
        s1 = decodeSyncState(encodeSyncState(s1));
        s2 = decodeSyncState(encodeSyncState(s2));

        // First n1 and n2 exchange Bloom filters
        let m1, m2;
        m1 = n1.generateSyncMessage(s1);
        m2 = n2.generateSyncMessage(s2);
        if (m1 === null) {
          throw new RangeError("message should not be null");
        }
        if (m2 === null) {
          throw new RangeError("message should not be null");
        }
        n1.receiveSyncMessage(s1, m2);
        n2.receiveSyncMessage(s2, m1);

        // Then n1 and n2 send each other their changes, except for the false positive
        m1 = n1.generateSyncMessage(s1);
        m2 = n2.generateSyncMessage(s2);
        if (m1 === null) {
          throw new RangeError("message should not be null");
        }
        if (m2 === null) {
          throw new RangeError("message should not be null");
        }
        n1.receiveSyncMessage(s1, m2);
        n2.receiveSyncMessage(s2, m1);
        assert(decodeSyncMessage(m1).changes.length > 0); // n1c1 and n1c2
        assert(decodeSyncMessage(m2).changes.length > 0); // only n2c2; change n2c1 is not sent

        // n3 is a node that doesn't have the missing change. Nevertheless n1 is going to ask n3 for it
        const n3 = create({ actor: "fedcba98" }),
          s13 = initSyncState(),
          s31 = initSyncState();
        sync(n1, n3, s13, s31);
        assert.deepStrictEqual(n1.getHeads(), [n1hash2]);
        assert.deepStrictEqual(n3.getHeads(), [n1hash2]);
      });
    });

    it("should not require an additional request when a false-positive depends on a true-negative", () => {
      // Scenario:                         ,-- n1c1 <-- n1c2 <-- n1c3
      // c0 <-- c1 <-- c2 <-- c3 <-- c4 <-+
      //                                   `-- n2c1 <-- n2c2 <-- n2c3
      // where n2c2 is a false positive in the Bloom filter containing {n1c1, n1c2, n1c3}.
      // lastSync is c4.
      let n1 = create({ actor: "01234567" }),
        n2 = create({ actor: "89abcdef" });
      let s1 = initSyncState(),
        s2 = initSyncState();
      let n1hash3, n2hash3;

      for (let i = 0; i < 5; i++) {
        n1.put("_root", "x", i);
        n1.commit("", 0);
      }
      sync(n1, n2, s1, s2);
      for (let i = 86; ; i++) {
        // search for false positive; see comment above
        const n1us1 = n1.clone("01234567");
        n1us1.put("_root", "x", `${i} @ n1`);
        n1us1.commit("", 0);

        const n2us1 = n2.clone("89abcdef");
        n2us1.put("_root", "x", `${i} @ n2`);
        n2us1.commit("", 0);

        //const n1us1 = Automerge.change(Automerge.clone(n1, {actorId: '01234567'}), {time: 0}, doc => doc.x = `${i} @ n1`)
        //const n2us1 = Automerge.change(Automerge.clone(n2, {actorId: '89abcdef'}), {time: 0}, doc => doc.x = `${i} @ n2`)
        const n1hash1 = n1us1.getHeads()[0];

        const n1us2 = n1us1.clone();
        n1us2.put("_root", "x", `${i + 1} @ n1`);
        n1us2.commit("", 0);

        const n2us2 = n2us1.clone();
        n2us2.put("_root", "x", `${i + 1} @ n2`);
        n2us2.commit("", 0);

        const n1hash2 = n1us2.getHeads()[0],
          n2hash2 = n2us2.getHeads()[0];

        const n1us3 = n1us2.clone();
        n1us3.put("_root", "x", `final @ n1`);
        n1us3.commit("", 0);

        const n2us3 = n2us2.clone();
        n2us3.put("_root", "x", `final @ n2`);
        n2us3.commit("", 0);

        n1hash3 = n1us3.getHeads()[0];
        n2hash3 = n2us3.getHeads()[0];

        if (
          new BloomFilter([n1hash1, n1hash2, n1hash3]).containsHash(n2hash2)
        ) {
          n1 = n1us3;
          n2 = n2us3;
          break;
        }
      }
      const bothHeads = [n1hash3, n2hash3].sort();
      s1 = decodeSyncState(encodeSyncState(s1));
      s2 = decodeSyncState(encodeSyncState(s2));
      sync(n1, n2, s1, s2);
      assert.deepStrictEqual(n1.getHeads(), bothHeads);
      assert.deepStrictEqual(n2.getHeads(), bothHeads);
    });

    it("should handle chains of false-positives", () => {
      // Scenario:                         ,-- c5
      // c0 <-- c1 <-- c2 <-- c3 <-- c4 <-+
      //                                   `-- n2c1 <-- n2c2 <-- n2c3
      // where n2c1 and n2c2 are both false positives in the Bloom filter containing {c5}.
      // lastSync is c4.
      const n1 = create({ actor: "01234567" });
      let n2 = create({ actor: "89abcdef" });
      let s1 = initSyncState(),
        s2 = initSyncState();

      for (let i = 0; i < 5; i++) {
        n1.put("_root", "x", i);
        n1.commit("", 0);
      }

      sync(n1, n2, s1, s2);

      n1.put("_root", "x", 5);
      n1.commit("", 0);

      for (let i = 2; ; i++) {
        // search for false positive; see comment above
        const n2us1 = n2.clone("89abcdef");
        n2us1.put("_root", "x", `${i} @ n2`);
        n2us1.commit("", 0);
        if (new BloomFilter(n1.getHeads()).containsHash(n2us1.getHeads()[0])) {
          n2 = n2us1;
          break;
        }
      }
      for (let i = 141; ; i++) {
        // search for false positive; see comment above
        const n2us2 = n2.clone("89abcdef");
        n2us2.put("_root", "x", `${i} again`);
        n2us2.commit("", 0);
        if (new BloomFilter(n1.getHeads()).containsHash(n2us2.getHeads()[0])) {
          n2 = n2us2;
          break;
        }
      }
      n2.put("_root", "x", `final @ n2`);
      n2.commit("", 0);

      const allHeads = [...n1.getHeads(), ...n2.getHeads()].sort();
      s1 = decodeSyncState(encodeSyncState(s1));
      s2 = decodeSyncState(encodeSyncState(s2));
      sync(n1, n2, s1, s2);
      assert.deepStrictEqual(n1.getHeads(), allHeads);
      assert.deepStrictEqual(n2.getHeads(), allHeads);
    });

    it("should allow the false-positive hash to be explicitly requested", () => {
      // Scenario:                                                            ,-- n1
      // c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8 <-- c9 <-+
      //                                                                      `-- n2
      // where n2 causes a false positive in the Bloom filter containing {n1}.
      let n1 = create({ actor: "01234567" }),
        n2 = create({ actor: "89abcdef" });
      let s1 = initSyncState(),
        s2 = initSyncState();
      let message;

      for (let i = 0; i < 10; i++) {
        n1.put("_root", "x", i);
        n1.commit("", 0);
      }

      sync(n1, n2, s1, s2);

      s1 = decodeSyncState(encodeSyncState(s1));
      s2 = decodeSyncState(encodeSyncState(s2));

      for (let i = 1; ; i++) {
        // brute-force search for false positive; see comment above
        const n1up = n1.clone("01234567");
        n1up.put("_root", "x", `${i} @ n1`);
        n1up.commit("", 0);
        const n2up = n1.clone("89abcdef");
        n2up.put("_root", "x", `${i} @ n2`);
        n2up.commit("", 0);

        // check if the bloom filter on n2 will believe n1 already has a particular hash
        // this will mean n2 won't offer that data to n2 by receiving a sync message from n1
        if (new BloomFilter(n1up.getHeads()).containsHash(n2up.getHeads()[0])) {
          n1 = n1up;
          n2 = n2up;
          break;
        }
      }

      // n1 creates a sync message for n2 with an ill-fated bloom
      message = n1.generateSyncMessage(s1);
      if (message === null) {
        throw new RangeError("message should not be null");
      }
      assert.strictEqual(decodeSyncMessage(message).changes.length, 0);

      // n2 receives it and DOESN'T send a change back
      n2.receiveSyncMessage(s2, message);
      message = n2.generateSyncMessage(s2);
      if (message === null) {
        throw new RangeError("message should not be null");
      }
      assert.strictEqual(decodeSyncMessage(message).changes.length, 0);

      // n1 should now realize it's missing that change and request it explicitly
      n1.receiveSyncMessage(s1, message);
      message = n1.generateSyncMessage(s1);
      if (message === null) {
        throw new RangeError("message should not be null");
      }
      assert.deepStrictEqual(decodeSyncMessage(message).need, n2.getHeads());

      // n2 should fulfill that request
      n2.receiveSyncMessage(s2, message);
      message = n2.generateSyncMessage(s2);
      if (message === null) {
        throw new RangeError("message should not be null");
      }
      assert.strictEqual(decodeSyncMessage(message).changes.length, 1);

      // n1 should apply the change and the two should now be in sync
      n1.receiveSyncMessage(s1, message);
      assert.deepStrictEqual(n1.getHeads(), n2.getHeads());
    });

    describe("protocol features", () => {
      it("should allow multiple Bloom filters", () => {
        // Scenario:           ,-- n1c1 <-- n1c2 <-- n1c3
        // c0 <-- c1 <-- c2 <-+--- n2c1 <-- n2c2 <-- n2c3
        //                     `-- n3c1 <-- n3c2 <-- n3c3
        // n1 has {c0, c1, c2, n1c1, n1c2, n1c3, n2c1, n2c2};
        // n2 has {c0, c1, c2, n1c1, n1c2, n2c1, n2c2, n2c3};
        // n3 has {c0, c1, c2, n3c1, n3c2, n3c3}.
        const n1 = create({ actor: "01234567" }),
          n2 = create({ actor: "89abcdef" }),
          n3 = create({ actor: "76543210" });
        let s13 = initSyncState();
        const s12 = initSyncState();
        const s21 = initSyncState();
        let s32 = initSyncState(),
          s31 = initSyncState(),
          s23 = initSyncState();
        let message1, message3;

        for (let i = 0; i < 3; i++) {
          n1.put("_root", "x", i);
          n1.commit("", 0);
        }

        // sync all 3 nodes
        sync(n1, n2, s12, s21); // eslint-disable-line no-unused-vars -- kept for consistency
        sync(n1, n3, s13, s31);
        sync(n3, n2, s32, s23);
        for (let i = 0; i < 2; i++) {
          n1.put("_root", "x", `${i} @ n1`);
          n1.commit("", 0);
        }
        for (let i = 0; i < 2; i++) {
          n2.put("_root", "x", `${i} @ n2`);
          n2.commit("", 0);
        }
        n1.applyChanges(n2.getChanges([]));
        n2.applyChanges(n1.getChanges([]));
        n1.put("_root", "x", `3 @ n1`);
        n1.commit("", 0);
        n2.put("_root", "x", `3 @ n2`);
        n2.commit("", 0);

        for (let i = 0; i < 3; i++) {
          n3.put("_root", "x", `${i} @ n3`);
          n3.commit("", 0);
        }
        const n1c3 = n1.getHeads()[0],
          n2c3 = n2.getHeads()[0],
          n3c3 = n3.getHeads()[0];
        s13 = decodeSyncState(encodeSyncState(s13));
        s31 = decodeSyncState(encodeSyncState(s31));
        s23 = decodeSyncState(encodeSyncState(s23));
        s32 = decodeSyncState(encodeSyncState(s32));

        // Now n3 concurrently syncs with n1 and n2. Doing this naively would result in n3 receiving
        // changes {n1c1, n1c2, n2c1, n2c2} twice (those are the changes that both n1 and n2 have, but
        // that n3 does not have). We want to prevent this duplication.
        message1 = n1.generateSyncMessage(s13); // message from n1 to n3
        if (message1 === null) {
          throw new RangeError("message should not be null");
        }
        assert.strictEqual(decodeSyncMessage(message1).changes.length, 0);
        n3.receiveSyncMessage(s31, message1);
        message3 = n3.generateSyncMessage(s31); // message from n3 to n1
        if (message3 === null) {
          throw new RangeError("message should not be null");
        }
        assert(decodeSyncMessage(message3).changes.length > 0); // {n3c1, n3c2, n3c3}
        n1.receiveSyncMessage(s13, message3);

        // Copy the Bloom filter received from n1 into the message sent from n3 to n2. This Bloom
        // filter indicates what changes n3 is going to receive from n1.
        message3 = n3.generateSyncMessage(s32); // message from n3 to n2
        if (message3 === null) {
          throw new RangeError("message should not be null");
        }
        const modifiedMessage = decodeSyncMessage(message3);
        modifiedMessage.have.push(decodeSyncMessage(message1).have[0]);
        assert.strictEqual(modifiedMessage.changes.length, 0);
        n2.receiveSyncMessage(s23, encodeSyncMessage(modifiedMessage));

        // n2 replies to n3, sending only n2c3 (the one change that n2 has but n1 doesn't)
        const message2 = n2.generateSyncMessage(s23);
        if (message2 === null) {
          throw new RangeError("message should not be null");
        }
        assert(decodeSyncMessage(message2).changes.length > 0); // {n2c3}
        n3.receiveSyncMessage(s32, message2);

        // n1 replies to n3
        message1 = n1.generateSyncMessage(s13);
        if (message1 === null) {
          throw new RangeError("message should not be null");
        }
        assert(decodeSyncMessage(message1).changes.length > 0); // {n1c1, n1c2, n1c3, n2c1, n2c2}
        n3.receiveSyncMessage(s31, message1);
        assert.deepStrictEqual(n3.getHeads(), [n1c3, n2c3, n3c3].sort());
      });

      it("should allow any change to be requested", () => {
        const n1 = create({ actor: "01234567" }),
          n2 = create({ actor: "89abcdef" });
        const s1 = initSyncState(),
          s2 = initSyncState();
        let message = null;

        for (let i = 0; i < 3; i++) {
          n1.put("_root", "x", i);
          n1.commit("", 0);
        }

        const lastSync = n1.getHeads();

        for (let i = 3; i < 6; i++) {
          n1.put("_root", "x", i);
          n1.commit("", 0);
        }

        sync(n1, n2, s1, s2);
        s1.lastSentHeads = []; // force generateSyncMessage to return a message even though nothing changed
        message = n1.generateSyncMessage(s1);
        if (message === null) {
          throw new RangeError("message should not be null");
        }
        const modMsg = decodeSyncMessage(message);
        modMsg.need = lastSync; // re-request change 2
        n2.receiveSyncMessage(s2, encodeSyncMessage(modMsg));
        message = n2.generateSyncMessage(s2);
        if (message === null) {
          throw new RangeError("message should not be null");
        }
        assert(decodeSyncMessage(message).changes.length > 0);
        assert.strictEqual(
          decodeChange(decodeSyncMessage(message).changes[0]).hash,
          lastSync[0],
        );
      });

      it("should ignore requests for a nonexistent change", () => {
        const n1 = create({ actor: "01234567" }),
          n2 = create({ actor: "89abcdef" });
        const s1 = initSyncState(),
          s2 = initSyncState();
        let message = null;

        for (let i = 0; i < 3; i++) {
          n1.put("_root", "x", i);
          n1.commit("", 0);
        }

        n2.applyChanges(n1.getChanges([]));
        // n2 will always generate at least one sync message to advertise it's
        // heads so we generate that message now. This means that we should
        // not generate a message responding to the request for a nonexistent
        // change because we already sent the first message
        n2.generateSyncMessage(s2);
        message = n1.generateSyncMessage(s1);
        if (message === null) {
          throw new RangeError("message should not be null");
        }
        message = decodeSyncMessage(message);
        message.need = [
          "0000000000000000000000000000000000000000000000000000000000000000",
        ];
        message = encodeSyncMessage(message);
        n2.receiveSyncMessage(s2, message);
        message = n2.generateSyncMessage(s2);
        assert.strictEqual(message, null);
      });

      it("should allow a subset of changes to be sent", () => {
        //       ,-- c1 <-- c2
        // c0 <-+
        //       `-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8
        const n1 = create({ actor: "01234567" }),
          n2 = create({ actor: "89abcdef" }),
          n3 = create({ actor: "76543210" });
        let s1 = initSyncState(),
          s2 = initSyncState();
        let msg;

        n1.put("_root", "x", 0);
        n1.commit("", 0);
        n3.applyChanges(n3.getChangesAdded(n1)); // merge()
        for (let i = 1; i <= 2; i++) {
          n1.put("_root", "x", i);
          n1.commit("", 0);
        }
        for (let i = 3; i <= 4; i++) {
          n3.put("_root", "x", i);
          n3.commit("", 0);
        }
        const c2 = n1.getHeads()[0],
          c4 = n3.getHeads()[0];
        n2.applyChanges(n2.getChangesAdded(n3)); // merge()

        // Sync n1 and n2, so their shared heads are {c2, c4}
        sync(n1, n2, s1, s2);
        s1 = decodeSyncState(encodeSyncState(s1));
        s2 = decodeSyncState(encodeSyncState(s2));
        assert.deepStrictEqual(s1.sharedHeads, [c2, c4].sort());
        assert.deepStrictEqual(s2.sharedHeads, [c2, c4].sort());

        // n2 and n3 apply {c5, c6, c7, c8}
        n3.put("_root", "x", 5);
        n3.commit("", 0);
        const change5 = n3.getLastLocalChange();
        if (change5 === null) throw new RangeError("no local change");
        n3.put("_root", "x", 6);
        n3.commit("", 0);
        const change6 = n3.getLastLocalChange(),
          c6 = n3.getHeads()[0];
        if (change6 === null) throw new RangeError("no local change");
        for (let i = 7; i <= 8; i++) {
          n3.put("_root", "x", i);
          n3.commit("", 0);
        }
        const c8 = n3.getHeads()[0];
        n2.applyChanges(n2.getChangesAdded(n3)); // merge()

        // Now n1 initiates a sync with n2, and n2 replies with {c5, c6}. n2 does not send {c7, c8}
        msg = n1.generateSyncMessage(s1);
        if (msg === null) {
          throw new RangeError("message should not be null");
        }
        n2.receiveSyncMessage(s2, msg);
        msg = n2.generateSyncMessage(s2);
        if (msg === null) {
          throw new RangeError("message should not be null");
        }
        const decodedMsg = decodeSyncMessage(msg);
        decodedMsg.changes = [change5, change6];
        msg = encodeSyncMessage(decodedMsg);
        const sentHashes: any = {};

        sentHashes[decodeChange(change5).hash] = true;
        sentHashes[decodeChange(change6).hash] = true;

        s2.sentHashes = sentHashes;
        n1.receiveSyncMessage(s1, msg);
        assert.deepStrictEqual(s1.sharedHeads, [c2, c6].sort());

        // n1 replies, confirming the receipt of {c5, c6} and requesting the remaining changes
        msg = n1.generateSyncMessage(s1);
        if (msg === null) {
          throw new RangeError("message should not be null");
        }
        n2.receiveSyncMessage(s2, msg);
        assert.deepStrictEqual(decodeSyncMessage(msg).need, [c8]);
        assert.deepStrictEqual(
          decodeSyncMessage(msg).have[0].lastSync,
          [c2, c6].sort(),
        );
        assert.deepStrictEqual(s1.sharedHeads, [c2, c6].sort());
        assert.deepStrictEqual(s2.sharedHeads, [c2, c6].sort());

        // n2 sends the remaining changes {c7, c8}
        msg = n2.generateSyncMessage(s2);
        if (msg === null) {
          throw new RangeError("message should not be null");
        }
        n1.receiveSyncMessage(s1, msg);
        assert(decodeSyncMessage(msg).changes.length > 0);
        assert.deepStrictEqual(s1.sharedHeads, [c2, c8].sort());
      });
    });

    it("can handle overlappying splices", () => {
      const doc = create();
      let mat: any = doc.materialize("/");
      doc.putObject("/", "text", "abcdefghij");
      doc.splice("/text", 2, 2, "00");
      doc.splice("/text", 3, 5, "11");
      mat = doc.applyPatches(mat);
      assert.deepEqual(mat.text, "ab011ij");
    });

    it("can handle utf16 text", () => {
      const doc = create();
      let mat: any = doc.materialize("/");

      doc.putObject("/", "width1", "AAAAAA");
      doc.putObject("/", "width2", "🐻🐻🐻🐻🐻🐻");
      doc.putObject("/", "mixed", "A🐻A🐻A🐻");

      assert.deepEqual(doc.length("/width1"), 6);
      assert.deepEqual(doc.length("/width2"), 12);
      assert.deepEqual(doc.length("/mixed"), 9);

      const heads1 = doc.getHeads();

      mat = doc.applyPatches(mat);

      const remote = load(doc.save());
      let r_mat: any = remote.materialize("/");

      assert.deepEqual(mat, {
        width1: "AAAAAA",
        width2: "🐻🐻🐻🐻🐻🐻",
        mixed: "A🐻A🐻A🐻",
      });
      assert.deepEqual(mat.width1.slice(2, 4), "AA");
      assert.deepEqual(mat.width2.slice(2, 4), "🐻");
      assert.deepEqual(mat.mixed.slice(1, 4), "🐻A");

      assert.deepEqual(r_mat, {
        width1: "AAAAAA",
        width2: "🐻🐻🐻🐻🐻🐻",
        mixed: "A🐻A🐻A🐻",
      });
      assert.deepEqual(r_mat.width1.slice(2, 4), "AA");
      assert.deepEqual(r_mat.width2.slice(2, 4), "🐻");
      assert.deepEqual(r_mat.mixed.slice(1, 4), "🐻A");

      doc.splice("/width1", 2, 2, "🐻");
      doc.splice("/width2", 2, 2, "A🐻A");
      doc.splice("/mixed", 3, 3, "X");
      assert.equal(doc.get("/mixed", 3), "X");

      mat = doc.applyPatches(mat);
      remote.loadIncremental(doc.saveIncremental());
      r_mat = remote.applyPatches(r_mat);

      assert.deepEqual(mat.width1, "AA🐻AA");
      assert.deepEqual(mat.width2, "🐻A🐻A🐻🐻🐻🐻");
      assert.deepEqual(mat.mixed, "A🐻XA🐻");

      assert.deepEqual(r_mat.width1, "AA🐻AA");
      assert.deepEqual(r_mat.width2, "🐻A🐻A🐻🐻🐻🐻");
      assert.deepEqual(r_mat.mixed, "A🐻XA🐻");
      assert.deepEqual(remote.length("/width1"), 6);
      assert.deepEqual(remote.length("/width2"), 14);
      assert.deepEqual(remote.length("/mixed"), 7);

      // when indexing in the middle of a multibyte char it indexes at the char after
      doc.splice("/width2", 4, 1, "X");
      mat = doc.applyPatches(mat);
      remote.loadIncremental(doc.saveIncremental());
      r_mat = remote.applyPatches(r_mat);

      assert.deepEqual(mat.width2, "🐻A🐻X🐻🐻🐻🐻");

      assert.deepEqual(doc.length("/width1", heads1), 6);
      assert.deepEqual(doc.length("/width2", heads1), 12);
      assert.deepEqual(doc.length("/mixed", heads1), 9);

      assert.deepEqual(doc.get("/mixed", 0), "A");
      assert.deepEqual(doc.get("/mixed", 1), "🐻");
      assert.deepEqual(doc.get("/mixed", 3), "X");
      assert.deepEqual(doc.get("/mixed", 1, heads1), "🐻");
      assert.deepEqual(doc.get("/mixed", 3, heads1), "A");
      assert.deepEqual(doc.get("/mixed", 4, heads1), "🐻");
    });

    it("can handle non-characters embedded in text", () => {
      const change: any = {
        ops: [
          { action: "makeText", obj: "_root", key: "bad_text", pred: [] },
          {
            action: "set",
            obj: "1@aaaa",
            elemId: "_head",
            insert: true,
            value: "A",
            pred: [],
          },
          {
            action: "set",
            obj: "1@aaaa",
            elemId: "2@aaaa",
            insert: true,
            value: "BBBBB",
            pred: [],
          },
          {
            action: "makeMap",
            obj: "1@aaaa",
            elemId: "3@aaaa",
            insert: true,
            pred: [],
          },
          {
            action: "set",
            obj: "1@aaaa",
            elemId: "4@aaaa",
            insert: true,
            value: "C",
            pred: [],
          },
        ],
        actor: "aaaa",
        seq: 1,
        startOp: 1,
        time: 0,
        message: null,
        deps: [],
      };
      const doc = load(encodeChange(change));
      const mat: any = doc.materialize("/");

      // multi - char strings appear as a span of strings
      // non strings appear as an object replacement unicode char
      assert.deepEqual(mat.bad_text, "ABBBBB\ufffcC");
      assert.deepEqual(doc.text("/bad_text"), "ABBBBB\ufffcC");
      assert.deepEqual(doc.materialize("/bad_text"), "ABBBBB\ufffcC");

      // deleting in the middle of a multi-byte character will delete after
      const doc1 = doc.fork();
      doc1.splice("/bad_text", 3, 3, "X");
      assert.deepEqual(doc1.text("/bad_text"), "ABBBBBX");

      // deleting in the middle of a multi-byte character will delete after
      const doc2 = doc.fork();
      doc2.splice("/bad_text", 3, 4, "X");
      assert.deepEqual(doc2.text("/bad_text"), "ABBBBBX");

      const doc3 = doc.fork();
      doc3.splice("/bad_text", 1, 7, "X");
      assert.deepEqual(doc3.text("/bad_text"), "AX");

      // inserting in the middle of a mutli-bytes span inserts after
      const doc4 = doc.fork();
      doc4.splice("/bad_text", 3, 0, "X");
      assert.deepEqual(doc4.text("/bad_text"), "ABBBBBX\ufffcC");

      // deleting into the middle of a multi-byte span deletes the whole thing
      const doc5 = doc.fork();
      doc5.splice("/bad_text", 0, 2, "X");
      assert.deepEqual(doc5.text("/bad_text"), "X\ufffcC");
    });

    it("should report whether the other end has our changes", () => {
      const left = create();
      left.put("/", "foo", "bar");

      const right = create();
      right.put("/", "baz", "qux");

      const leftSync = initSyncState();
      const rightSync = initSyncState();

      while (!left.hasOurChanges(leftSync) || !right.hasOurChanges(rightSync)) {
        let quiet = true;
        let msg = left.generateSyncMessage(leftSync);
        if (msg) {
          right.receiveSyncMessage(rightSync, msg);
          quiet = false;
        }

        msg = right.generateSyncMessage(rightSync);
        if (msg) {
          left.receiveSyncMessage(leftSync, msg);
          quiet = false;
        }
        if (quiet) {
          throw new Error(
            "no message generated but the sync states think we're done",
          );
        }
      }

      assert(left.hasOurChanges(leftSync));
      assert(right.hasOurChanges(rightSync));
    });
  });

  describe("the topoHistoryTraversal function", () => {
    it("should return a topological traverssal of the hashes of the changes", () => {
      const doc = create({ actor: "aaaaaa" });
      doc.put("/", "foo", "bar");
      let hash1 = decodeChange(doc.getLastLocalChange()!).hash;

      const doc2 = doc.clone("bbbbbb");

      doc.put("/", "baz", "qux");
      let hash2 = decodeChange(doc.getLastLocalChange()!).hash;

      doc2.put("/", "baz", "qux");
      let hash3 = decodeChange(doc2.getLastLocalChange()!).hash;

      doc.merge(doc2);

      let hashes = [hash1, hash2, hash3];
      const traversal = doc.topoHistoryTraversal();
      assert.deepStrictEqual(hashes, traversal);
    });
  });

  describe("the getDecodedChangeByHash function", () => {
    it("should return the change with the given heads", () => {
      const doc = create();
      doc.put("/", "foo", "bar");
      let hash = doc.topoHistoryTraversal()[0];
      let change = doc.getDecodedChangeByHash(hash)!;
      assert.deepStrictEqual(change.hash, hash);
      assert.deepStrictEqual(change.ops, [
        {
          action: "set",
          key: "foo",
          obj: "_root",
          pred: [],
          value: "bar",
        },
      ]);
    });
  });

  describe("the stats function", () => {
    it("should return the number of changes and the number of ops", () => {
      const doc = create();
      doc.put("/", "foo", "bar");
      doc.commit();
      doc.put("/", "baz", "qux");
      doc.commit();
      const stats = doc.stats();
      assert.equal(stats.numChanges, 2);
      assert.equal(stats.numOps, 2);
      assert.equal(stats.numActors, 1);
    });
  });
  describe("change metadata", () => {
    it("mirrors decoded changes", () => {
      const doc = create();
      doc.put("/", "foo", "bar");
      doc.commit();
      doc.put("/", "baz", "qux");
      doc.commit();
      let changes = doc.getChanges([]).map(decodeChange);
      let meta = doc.getChangesMeta([]);
      assert.equal(changes.length, 2);
      assert.equal(meta.length, 2);
      for (let i = 0; i < 2; i++) {
        assert.equal(changes[i].actor, meta[i].actor);
        assert.equal(changes[i].hash, meta[i].hash);
        assert.equal(changes[i].message, meta[i].message);
        assert.equal(changes[i].time, meta[i].time);
        assert.deepEqual(changes[i].deps, meta[i].deps);
        assert.deepEqual(changes[i].startOp, meta[i].startOp);
      }
    });
  });
  describe("author", () => {
    it("author can be assigned", () => {
      const doc = create();
      doc.put("/", "key", "val1");
      doc.commit();
      const actor1 = doc.getActorId();
      doc.setAuthor("ffff");
      const actor2 = doc.getActorId();
      assert.notEqual(actor1, actor2)
      doc.put("/", "key", "val2");
      doc.commit();
      let change1 = decodeChange(doc.getLastLocalChange() as Uint8Array)
      assert.equal(change1.author, "ffff");
      doc.put("/", "key", "val3");
      doc.commit();
      let change2 = decodeChange(doc.getLastLocalChange() as Uint8Array)
      assert.equal(change2.author, undefined);
      assert.deepEqual(doc.getAuthors(),["ffff"]);
      assert.equal(doc.getAuthorForActor(actor2),"ffff");
      assert.deepEqual(doc.getActorsForAuthor("ffff"),[actor2]);
    });
  });
});
