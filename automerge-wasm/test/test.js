
const assert = require('assert')
const util = require('util')
const Automerge = require('..')
const { MAP, LIST, TEXT } = Automerge

// str to uint8array
function en(str) {
  return new TextEncoder('utf8').encode(str)
}
// uint8array to str
function de(bytes) {
  return new TextDecoder('utf8').decode(bytes);
}

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
      doc.commit()
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

      doc.set(root, "hello", "world")
      doc.set(root, "number1", 5, "uint")
      doc.set(root, "number2", 5)
      doc.set(root, "number3", 5.5)
      doc.set(root, "number4", 5.5, "f64")
      doc.set(root, "number5", 5.5, "int")
      doc.set(root, "bool", true)

      result = doc.value(root,"hello")
      assert.deepEqual(result,["str","world"])

      result = doc.value(root,"number1")
      assert.deepEqual(result,["uint",5])

      result = doc.value(root,"number2")
      assert.deepEqual(result,["int",5])

      result = doc.value(root,"number3")
      assert.deepEqual(result,["f64",5.5])

      result = doc.value(root,"number4")
      assert.deepEqual(result,["f64",5.5])

      result = doc.value(root,"number5")
      assert.deepEqual(result,["int",5])

      result = doc.value(root,"bool")
      assert.deepEqual(result,["boolean",true])

      doc.set(root, "bool", false, "boolean")

      result = doc.value(root,"bool")
      assert.deepEqual(result,["boolean",false])
    })

    it('should be able to use bytes', () => {
      let doc = Automerge.init()
      doc.set("_root","data1", new Uint8Array([10,11,12]));
      doc.set("_root","data2", new Uint8Array([13,14,15]), "bytes");
      let value1 = doc.value("_root", "data1")
      assert.deepEqual(value1, ["bytes", new Uint8Array([10,11,12])]);
      let value2 = doc.value("_root", "data2")
      assert.deepEqual(value2, ["bytes", new Uint8Array([13,14,15])]);
    })

    it('should be able to make sub objects', () => {
      let doc = Automerge.init()
      let root = "_root"
      let result

      let submap = doc.set(root, "submap", MAP)
      doc.set(submap, "number", 6, "uint")
      assert.strictEqual(doc.pending_ops(),2)

      result = doc.value(root,"submap")
      assert.deepEqual(result,["map",submap])

      result = doc.value(submap,"number")
      assert.deepEqual(result,["uint",6])
    })

    it('should be able to make lists', () => {
      let doc = Automerge.init()
      let root = "_root"

      let submap = doc.set(root, "numbers", LIST)
      doc.insert(submap, 0, "a");
      doc.insert(submap, 1, "b");
      doc.insert(submap, 2, "c");
      doc.insert(submap, 0, "z");

      assert.deepEqual(doc.value(submap, 0),["str","z"])
      assert.deepEqual(doc.value(submap, 1),["str","a"])
      assert.deepEqual(doc.value(submap, 2),["str","b"])
      assert.deepEqual(doc.value(submap, 3),["str","c"])
      assert.deepEqual(doc.length(submap),4)

      doc.set(submap, 2, "b v2");

      assert.deepEqual(doc.value(submap, 2),["str","b v2"])
      assert.deepEqual(doc.length(submap),4)
    })

    it('should be able delete non-existant props', () => {
      let doc = Automerge.init()

      doc.set("_root", "foo","bar")
      doc.set("_root", "bip","bap")
      let heads1 = doc.commit()

      assert.deepEqual(doc.keys("_root"),["bip","foo"])

      doc.del("_root", "foo")
      doc.del("_root", "baz")
      let heads2 = doc.commit()

      assert.deepEqual(doc.keys("_root"),["bip"])
      assert.deepEqual(doc.keys("_root", heads1),["bip", "foo"])
      assert.deepEqual(doc.keys("_root", heads2),["bip"])
    })

    it('should be able to del', () => {
      let doc = Automerge.init()
      let root = "_root"

      doc.set(root, "xxx", "xxx");
      assert.deepEqual(doc.value(root, "xxx"),["str","xxx"])
      doc.del(root, "xxx");
      assert.deepEqual(doc.value(root, "xxx"),[])
    })

    it('should be able to use counters', () => {
      let doc = Automerge.init()
      let root = "_root"

      doc.set(root, "counter", 10, "counter");
      assert.deepEqual(doc.value(root, "counter"),["counter",10])
      doc.inc(root, "counter", 10);
      assert.deepEqual(doc.value(root, "counter"),["counter",20])
      doc.inc(root, "counter", -5);
      assert.deepEqual(doc.value(root, "counter"),["counter",15])
    })

    it('should be able to splice text', () => {
      let doc = Automerge.init()
      let root = "_root";

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
    })

    it('should be able save all or incrementally', () => {
      let doc = Automerge.init()

      doc.set("_root", "foo", 1)

      let save1 = doc.save()

      doc.set("_root", "bar", 2)

      let saveMidway = doc.clone().save();

      let save2 = doc.saveIncremental();

      doc.set("_root", "baz", 3);

      let save3 = doc.saveIncremental();

      let saveA = doc.save();
      let saveB = new Uint8Array([... save1, ...save2, ...save3]);

      assert.notDeepEqual(saveA, saveB);

      let docA = Automerge.load(saveA);
      let docB = Automerge.load(saveB);
      let docC = Automerge.load(saveMidway)
      docC.loadIncremental(save3)

      assert.deepEqual(docA.keys("_root"), docB.keys("_root"));
      assert.deepEqual(docA.save(), docB.save());
      assert.deepEqual(docA.save(), docC.save());
    })

    it('should be able to splice text', () => {
      let doc = Automerge.init()
      let text = doc.set("_root", "text", TEXT);
      doc.splice(text, 0, 0, "hello world");
      let heads1 = doc.commit();
      doc.splice(text, 6, 0, "big bad ");
      let heads2 = doc.commit();
      assert.strictEqual(doc.text(text), "hello big bad world")
      assert.strictEqual(doc.length(text), 19)
      assert.strictEqual(doc.text(text, heads1), "hello world")
      assert.strictEqual(doc.length(text, heads1), 11)
      assert.strictEqual(doc.text(text, heads2), "hello big bad world")
      assert.strictEqual(doc.length(text, heads2), 19)
    })

    it('local inc increments all visible counters in a map', () => {
      let doc1 = Automerge.init("aaaa")
      doc1.set("_root", "hello", "world")
      let doc2 = Automerge.load(doc1.save(), "bbbb");
      let doc3 = Automerge.load(doc1.save(), "cccc");
      doc1.set("_root", "cnt", 20)
      doc2.set("_root", "cnt", 0, "counter")
      doc3.set("_root", "cnt", 10, "counter")
      doc1.applyChanges(doc2.getChanges(doc1.getHeads()))
      doc1.applyChanges(doc3.getChanges(doc1.getHeads()))
      let result = doc1.values("_root", "cnt")
      assert.deepEqual(result,[
        ['counter',10,'2@cccc'],
        ['counter',0,'2@bbbb'],
        ['int',20,'2@aaaa']
      ])
      doc1.inc("_root", "cnt", 5)
      result = doc1.values("_root", "cnt")
      assert.deepEqual(result, [
        [ 'counter', 15, '2@cccc' ], [ 'counter', 5, '2@bbbb' ]
      ])

      let save1 = doc1.save()
      let doc4 = Automerge.load(save1)
      assert.deepEqual(doc4.save(), save1);
    })

    it('local inc increments all visible counters in a sequence', () => {
      let doc1 = Automerge.init("aaaa")
      let seq = doc1.set("_root", "seq", LIST)
      doc1.insert(seq, 0, "hello")
      let doc2 = Automerge.load(doc1.save(), "bbbb");
      let doc3 = Automerge.load(doc1.save(), "cccc");
      doc1.set(seq, 0, 20)
      doc2.set(seq, 0, 0, "counter")
      doc3.set(seq, 0, 10, "counter")
      doc1.applyChanges(doc2.getChanges(doc1.getHeads()))
      doc1.applyChanges(doc3.getChanges(doc1.getHeads()))
      let result = doc1.values(seq, 0)
      assert.deepEqual(result,[
        ['counter',10,'3@cccc'],
        ['counter',0,'3@bbbb'],
        ['int',20,'3@aaaa']
      ])
      doc1.inc(seq, 0, 5)
      result = doc1.values(seq, 0)
      assert.deepEqual(result, [
        [ 'counter', 15, '3@cccc' ], [ 'counter', 5, '3@bbbb' ]
      ])

      let save = doc1.save()
      let doc4 = Automerge.load(save)
      assert.deepEqual(doc4.save(), save);
    })

    it('only returns an object id when objects are created', () => {
      let doc = Automerge.init("aaaa")
      let r1 = doc.set("_root","foo","bar")
      let r2 = doc.set("_root","list",LIST)
      let r3 = doc.set("_root","counter",10, "counter")
      let r4 = doc.inc("_root","counter",1)
      let r5 = doc.del("_root","counter")
      let r6 = doc.insert(r2,0,10);
      let r7 = doc.insert(r2,0,MAP);
      let r8 = doc.splice(r2,1,0,["a","b","c"]);
      let r9 = doc.splice(r2,1,0,["a",LIST,MAP,"d"]);
      assert.deepEqual(r1,null);
      assert.deepEqual(r2,"2@aaaa");
      assert.deepEqual(r3,null);
      assert.deepEqual(r4,null);
      assert.deepEqual(r5,null);
      assert.deepEqual(r6,null);
      assert.deepEqual(r7,"7@aaaa");
      assert.deepEqual(r8,null);
      assert.deepEqual(r9,["12@aaaa","13@aaaa"]);
    })

  })
})
