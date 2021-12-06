
const assert = require('assert')
const util = require('util')
const Automerge = require('..')
const { MAP, LIST } = Automerge

describe('Automerge', () => {
  describe('basics', () => {
    it('should init clone and free', () => {
      let doc1 = Automerge.init()
      let doc2 = doc1.clone()
      doc1.free()
      doc2.free()
    })

    it('should be able to start and commit', () => {
      let doc = Automerge.init()
      doc.begin()
      doc.commit()
    })

    it('calling begin inside a transaction should throw an error', () => {
      let doc = Automerge.init()
      doc.begin()
      assert.throws(() => { doc.begin() }, Error);
    })

    it('calling commit outside a transaction should throw an error', () => {
      let doc = Automerge.init()
      assert.throws(() => { doc.commit() }, Error);
    })

    it('getting a nonexistant prop does not throw an error', () => {
      let doc = Automerge.init()
      let root = "_root"
      let result = doc.value(root,"hello")
      assert.deepEqual(result,[])
    })

    it('should be able to set and get a simple value', () => {
      let doc = Automerge.init()
      let root = "_root"
      let result

      doc.begin()
      doc.set(root, "hello", "world")
      doc.set(root, "number", 5, "uint")
      doc.commit()

      result = doc.value(root,"hello")
      assert.deepEqual(result,["str","world"])

      result = doc.value(root,"number")
      assert.deepEqual(result,["uint",5])
    })

    it('should be able to use bytes', () => {
      let doc = Automerge.init()
      doc.begin()
      doc.set("_root","data", new Uint8Array([10,11,12]));
      doc.commit()
      let value = doc.value("_root", "data")
      assert.deepEqual(value, ["bytes", new Uint8Array([10,11,12])]);
    })

    it('should be able to make sub objects', () => {
      let doc = Automerge.init()
      let root = "_root"
      let result

      doc.begin()
      let submap = doc.set(root, "submap", MAP)
      doc.set(submap, "number", 6, "uint")
      assert.strictEqual(doc.pending_ops(),2)
      doc.commit()

      result = doc.value(root,"submap")
      assert.deepEqual(result,["map",submap])

      result = doc.value(submap,"number")
      assert.deepEqual(result,["uint",6])
    })

    it('should be able to make lists', () => {
      let doc = Automerge.init()
      let root = "_root"

      doc.begin()
      let submap = doc.set(root, "numbers", LIST)
      doc.insert(submap, 0, "a");
      doc.insert(submap, 1, "b");
      doc.insert(submap, 2, "c");
      doc.insert(submap, 0, "z");
      doc.commit()
      assert.deepEqual(doc.value(submap, 0),["str","z"])
      assert.deepEqual(doc.value(submap, 1),["str","a"])
      assert.deepEqual(doc.value(submap, 2),["str","b"])
      assert.deepEqual(doc.value(submap, 3),["str","c"])
      assert.deepEqual(doc.length(submap),4)
    })

    it('should be able to make lists', () => {
      let doc = Automerge.init()
      doc.begin()
      doc.set("_root", "foo","bar")
      doc.del("_root", "foo")
      doc.del("_root", "baz")
      doc.commit()
    })

    it('should be able to del', () => {
      let doc = Automerge.init()
      let root = "_root"

      doc.begin()
      doc.set(root, "xxx", "xxx");
      assert.deepEqual(doc.value(root, "xxx"),["str","xxx"])
      doc.del(root, "xxx");
      assert.deepEqual(doc.value(root, "xxx"),[])
      doc.commit()
    })

    it('should be able to use counters', () => {
      let doc = Automerge.init()
      let root = "_root"

      doc.begin()
      doc.set(root, "counter", 10, "counter");
      assert.deepEqual(doc.value(root, "counter"),["counter",10])
      doc.inc(root, "counter", 10);
      assert.deepEqual(doc.value(root, "counter"),["counter",20])
      doc.inc(root, "counter", -5);
      assert.deepEqual(doc.value(root, "counter"),["counter",15])
      doc.commit()
    })

    it('should be able to splice text', () => {
      let doc = Automerge.init()
      let root = "_root";

      doc.begin()
      let text = doc.set(root, "text", Automerge.TEXT);
      doc.splice(text, 0, 0, "hello ")
      doc.splice(text, 6, 0, ["w","o","r","l","d"])
      doc.splice(text, 11, 0, [["str","!"],["str","?"]])
      assert.deepEqual(doc.value(text, 0),["str","h"])
      assert.deepEqual(doc.value(text, 1),["str","e"])
      assert.deepEqual(doc.value(text, 9),["str","l"])
      assert.deepEqual(doc.value(text, 10),["str","d"])
      assert.deepEqual(doc.value(text, 11),["str","!"])
      assert.deepEqual(doc.value(text, 12),["str","?"])
      doc.commit()
    })

    it('should be able save all or incrementally', () => {
      let doc = Automerge.init()

      doc.begin()
      doc.set("_root", "foo", 1)
      doc.commit()

      let save1 = doc.save()

      doc.begin()
      doc.set("_root", "bar", 2)
      doc.commit()

      let save2 = doc.save_incremental()

      doc.begin()
      doc.set("_root", "baz", 3)
      doc.commit()

      let save3 = doc.save_incremental()

      let saveA = doc.save();
      let saveB = new Uint8Array([... save1, ...save2, ...save3]);

      assert.notDeepEqual(saveA, saveB);

      let docA = Automerge.load(saveA);
      let docB = Automerge.load(saveB);

      assert.deepEqual(docA.keys("_root"), docB.keys("_root"));
      assert.deepEqual(docA.save(), docB.save());
    })
  })
})
