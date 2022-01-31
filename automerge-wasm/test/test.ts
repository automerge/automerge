import { describe, it } from 'mocha';
//@ts-ignore
import assert from 'assert'
//@ts-ignore
import { BloomFilter } from './helpers/sync'
import { create, loadDoc, SyncState, Automerge, MAP, LIST, TEXT, encodeChange, decodeChange, initSyncState, decodeSyncMessage, decodeSyncState, encodeSyncState, encodeSyncMessage } from '../dev/index'
import { DecodedSyncMessage } from '../index';
import { Hash } from '../dev/index';

function sync(a: Automerge, b: Automerge, aSyncState = initSyncState(), bSyncState = initSyncState()) {
  const MAX_ITER = 10
  let aToBmsg = null, bToAmsg = null, i = 0
  do {
    aToBmsg = a.generateSyncMessage(aSyncState)
    bToAmsg = b.generateSyncMessage(bSyncState)

    if (aToBmsg) {
      b.receiveSyncMessage(bSyncState, aToBmsg)
    }
    if (bToAmsg) {
      a.receiveSyncMessage(aSyncState, bToAmsg)
    }

    if (i++ > MAX_ITER) {
      throw new Error(`Did not synchronize within ${MAX_ITER} iterations`)
    }
  } while (aToBmsg || bToAmsg)
}

describe('Automerge', () => {
  describe('basics', () => {
    it('should init clone and free', () => {
      let doc1 = create()
      let doc2 = doc1.clone()
      doc1.free()
      doc2.free()
    })

    it('should be able to start and commit', () => {
      let doc = create()
      doc.commit()
      doc.free()
    })

    it('getting a nonexistant prop does not throw an error', () => {
      let doc = create()
      let root = "_root"
      let result = doc.value(root,"hello")
      assert.deepEqual(result,undefined)
      doc.free()
    })

    it('should be able to set and get a simple value', () => {
      let doc : Automerge = create("aabbcc")
      let root = "_root"
      let result

      doc.set(root, "hello", "world")
      doc.set(root, "number1", 5, "uint")
      doc.set(root, "number2", 5)
      doc.set(root, "number3", 5.5)
      doc.set(root, "number4", 5.5, "f64")
      doc.set(root, "number5", 5.5, "int")
      doc.set(root, "bool", true)
      doc.set(root, "time1", 1000, "timestamp")
      doc.set(root, "time2", new Date(1001))
      doc.set(root, "list", LIST);
      doc.set(root, "null", null)

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

      result = doc.value(root,"time1")
      assert.deepEqual(result,["timestamp",new Date(1000)])

      result = doc.value(root,"time2")
      assert.deepEqual(result,["timestamp",new Date(1001)])

      result = doc.value(root,"list")
      assert.deepEqual(result,["list","10@aabbcc"]);

      result = doc.value(root,"null")
      assert.deepEqual(result,["null",null]);

      doc.free()
    })

    it('should be able to use bytes', () => {
      let doc = create()
      doc.set("_root","data1", new Uint8Array([10,11,12]));
      doc.set("_root","data2", new Uint8Array([13,14,15]), "bytes");
      let value1 = doc.value("_root", "data1")
      assert.deepEqual(value1, ["bytes", new Uint8Array([10,11,12])]);
      let value2 = doc.value("_root", "data2")
      assert.deepEqual(value2, ["bytes", new Uint8Array([13,14,15])]);
      doc.free()
    })

    it('should be able to make sub objects', () => {
      let doc = create()
      let root = "_root"
      let result

      let submap = doc.set(root, "submap", MAP)
      if (!submap) throw new Error('should be not null')
      doc.set(submap, "number", 6, "uint")
      assert.strictEqual(doc.pendingOps(),2)

      result = doc.value(root,"submap")
      assert.deepEqual(result,["map",submap])

      result = doc.value(submap,"number")
      assert.deepEqual(result,["uint",6])
      doc.free()
    })

    it('should be able to make lists', () => {
      let doc = create()
      let root = "_root"

      let submap = doc.set(root, "numbers", LIST)
      if (!submap) throw new Error('should be not null')
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
      doc.free()
    })

    it('lists have insert, set, splice, and push ops', () => {
      let doc = create()
      let root = "_root"

      let submap = doc.set(root, "letters", LIST)
      if (!submap) throw new Error('should be not null')
      doc.insert(submap, 0, "a");
      doc.insert(submap, 0, "b");
      assert.deepEqual(doc.toJS(), { letters: ["b", "a" ] })
      doc.push(submap, "c");
      assert.deepEqual(doc.toJS(), { letters: ["b", "a", "c" ] })
      doc.push(submap, 3, "timestamp");
      assert.deepEqual(doc.toJS(), { letters: ["b", "a", "c", new Date(3) ] })
      doc.splice(submap, 1, 1, ["d","e","f"]);
      assert.deepEqual(doc.toJS(), { letters: ["b", "d", "e", "f", "c", new Date(3) ] })
      doc.set(submap, 0, "z");
      assert.deepEqual(doc.toJS(), { letters: ["z", "d", "e", "f", "c", new Date(3) ] })
      assert.deepEqual(doc.length(submap),6)

      doc.free()
    })

    it('should be able delete non-existant props', () => {
      let doc = create()

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
      doc.free()
    })

    it('should be able to del', () => {
      let doc = create()
      let root = "_root"

      doc.set(root, "xxx", "xxx");
      assert.deepEqual(doc.value(root, "xxx"),["str","xxx"])
      doc.del(root, "xxx");
      assert.deepEqual(doc.value(root, "xxx"),undefined)
      doc.free()
    })

    it('should be able to use counters', () => {
      let doc = create()
      let root = "_root"

      doc.set(root, "counter", 10, "counter");
      assert.deepEqual(doc.value(root, "counter"),["counter",10])
      doc.inc(root, "counter", 10);
      assert.deepEqual(doc.value(root, "counter"),["counter",20])
      doc.inc(root, "counter", -5);
      assert.deepEqual(doc.value(root, "counter"),["counter",15])
      doc.free()
    })

    it('should be able to splice text', () => {
      let doc = create()
      let root = "_root";

      let text = doc.set(root, "text", TEXT);
      if (!text) throw new Error('should not be undefined')
      doc.splice(text, 0, 0, "hello ")
      doc.splice(text, 6, 0, ["w","o","r","l","d"])
      doc.splice(text, 11, 0, [["str","!"],["str","?"]])
      assert.deepEqual(doc.value(text, 0),["str","h"])
      assert.deepEqual(doc.value(text, 1),["str","e"])
      assert.deepEqual(doc.value(text, 9),["str","l"])
      assert.deepEqual(doc.value(text, 10),["str","d"])
      assert.deepEqual(doc.value(text, 11),["str","!"])
      assert.deepEqual(doc.value(text, 12),["str","?"])
      doc.free()
    })

    it('should be able save all or incrementally', () => {
      let doc = create()

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

      let docA = loadDoc(saveA);
      let docB = loadDoc(saveB);
      let docC = loadDoc(saveMidway)
      docC.loadIncremental(save3)

      assert.deepEqual(docA.keys("_root"), docB.keys("_root"));
      assert.deepEqual(docA.save(), docB.save());
      assert.deepEqual(docA.save(), docC.save());
      doc.free()
      docA.free()
      docB.free()
      docC.free()
    })

    it('should be able to splice text', () => {
      let doc = create()
      let text = doc.set("_root", "text", TEXT);
      if (!text) throw new Error('should not be undefined')
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
      doc.free()
    })

    it('local inc increments all visible counters in a map', () => {
      let doc1 = create("aaaa")
      doc1.set("_root", "hello", "world")
      let doc2 = loadDoc(doc1.save(), "bbbb");
      let doc3 = loadDoc(doc1.save(), "cccc");
      doc1.set("_root", "cnt", 20)
      doc2.set("_root", "cnt", 0, "counter")
      doc3.set("_root", "cnt", 10, "counter")
      doc1.applyChanges(doc2.getChanges(doc1.getHeads()))
      doc1.applyChanges(doc3.getChanges(doc1.getHeads()))
      let result = doc1.values("_root", "cnt")
      assert.deepEqual(result,[
        ['int',20,'2@aaaa'],
        ['counter',0,'2@bbbb'],
        ['counter',10,'2@cccc'],
      ])
      doc1.inc("_root", "cnt", 5)
      result = doc1.values("_root", "cnt")
      assert.deepEqual(result, [
        [ 'counter', 5, '2@bbbb' ],
        [ 'counter', 15, '2@cccc' ],
      ])

      let save1 = doc1.save()
      let doc4 = loadDoc(save1)
      assert.deepEqual(doc4.save(), save1);
      doc1.free()
      doc2.free()
      doc3.free()
      doc4.free()
    })

    it('local inc increments all visible counters in a sequence', () => {
      let doc1 = create("aaaa")
      let seq = doc1.set("_root", "seq", LIST)
      if (!seq) throw new Error('Should not be undefined')
      doc1.insert(seq, 0, "hello")
      let doc2 = loadDoc(doc1.save(), "bbbb");
      let doc3 = loadDoc(doc1.save(), "cccc");
      doc1.set(seq, 0, 20)
      doc2.set(seq, 0, 0, "counter")
      doc3.set(seq, 0, 10, "counter")
      doc1.applyChanges(doc2.getChanges(doc1.getHeads()))
      doc1.applyChanges(doc3.getChanges(doc1.getHeads()))
      let result = doc1.values(seq, 0)
      assert.deepEqual(result,[
        ['int',20,'3@aaaa'],
        ['counter',0,'3@bbbb'],
        ['counter',10,'3@cccc'],
      ])
      doc1.inc(seq, 0, 5)
      result = doc1.values(seq, 0)
      assert.deepEqual(result, [
        [ 'counter', 5, '3@bbbb' ],
        [ 'counter', 15, '3@cccc' ],
      ])

      let save = doc1.save()
      let doc4 = loadDoc(save)
      assert.deepEqual(doc4.save(), save);
      doc1.free()
      doc2.free()
      doc3.free()
      doc4.free()
    })

    it('only returns an object id when objects are created', () => {
      let doc = create("aaaa")
      let r1 = doc.set("_root","foo","bar")
      let r2 = doc.set("_root","list",LIST)
      let r3 = doc.set("_root","counter",10, "counter")
      let r4 = doc.inc("_root","counter",1)
      let r5 = doc.del("_root","counter")
      if (!r2) throw new Error('should not be undefined')
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
      doc.free()
    })

    it('objects without properties are preserved', () => {
      let doc1 = create("aaaa")
      let a = doc1.set("_root","a",MAP);
      if (!a) throw new Error('should not be undefined')
      let b = doc1.set("_root","b",MAP);
      if (!b) throw new Error('should not be undefined')
      let c = doc1.set("_root","c",MAP);
      if (!c) throw new Error('should not be undefined')
      let d = doc1.set(c,"d","dd");
      let saved = doc1.save();
      let doc2 = loadDoc(saved);
      assert.deepEqual(doc2.value("_root","a"),["map",a])
      assert.deepEqual(doc2.keys(a),[])
      assert.deepEqual(doc2.value("_root","b"),["map",b])
      assert.deepEqual(doc2.keys(b),[])
      assert.deepEqual(doc2.value("_root","c"),["map",c])
      assert.deepEqual(doc2.keys(c),["d"])
      assert.deepEqual(doc2.value(c,"d"),["str","dd"])
      doc1.free()
      doc2.free()
    })

    it('should handle marks [..]', () => {
      let doc = create()
       let list = doc.set("_root", "list", TEXT)
      if (!list) throw new Error('should not be undefined')
       doc.splice(list, 0, 0, "aaabbbccc")
       doc.mark(list, "[3..6]", "bold" , true)
      let spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaa', [ [ 'bold', 'boolean', true ] ], 'bbb', [], 'ccc' ]);
      doc.insert(list, 6, "A")
      doc.insert(list, 3, "A")
      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaaA', [ [ 'bold', 'boolean', true ] ], 'bbb', [], 'Accc' ]);
    })

    it('should handle marks with deleted ends [..]', () => {
      let doc = create()
      let list = doc.set("_root", "list", TEXT)
      if (!list) throw new Error('should not be undefined')

      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "[3..6]", "bold" , true)
      let spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaa', [ [ 'bold', 'boolean', true ] ], 'bbb', [], 'ccc' ]);
      doc.del(list,5);
      doc.del(list,5);
      doc.del(list,2);
      doc.del(list,2);
      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aa', [ [ 'bold', 'boolean', true ] ], 'b', [], 'cc' ])
      doc.insert(list, 3, "A")
      doc.insert(list, 2, "A")
      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaA', [ [ 'bold', 'boolean', true ] ], 'b', [], 'Acc' ])
    })

    it('should handle sticky marks (..)', () => {
      let doc = create()
      let list = doc.set("_root", "list", TEXT)
      if (!list) throw new Error('should not be undefined')
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "(3..6)", "bold" , true)
      let spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaa', [ [ 'bold', 'boolean', true ] ], 'bbb', [], 'ccc' ]);
      doc.insert(list, 6, "A")
      doc.insert(list, 3, "A")
      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaa', [ [ 'bold', 'boolean', true ] ], 'AbbbA', [], 'ccc' ]);
    })

    it('should handle sticky marks with deleted ends (..)', () => {
      let doc = create()
      let list = doc.set("_root", "list", TEXT)
      if (!list) throw new Error('should not be undefined')
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "(3..6)", "bold" , true)
      let spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaa', [ [ 'bold', 'boolean', true ] ], 'bbb', [], 'ccc' ]);
      doc.del(list,5);
      doc.del(list,5);
      doc.del(list,2);
      doc.del(list,2);
      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aa', [ [ 'bold', 'boolean', true ] ], 'b', [], 'cc' ])
      doc.insert(list, 3, "A")
      doc.insert(list, 2, "A")
      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aa', [ [ 'bold', 'boolean', true ] ], 'AbA', [], 'cc' ])

      // make sure save/load can handle marks

      let doc2 = loadDoc(doc.save())
      spans = doc2.spans(list);
      assert.deepStrictEqual(spans, [ 'aa', [ [ 'bold', 'boolean', true ] ], 'AbA', [], 'cc' ])

      assert.deepStrictEqual(doc.getHeads(), doc2.getHeads())
      assert.deepStrictEqual(doc.save(), doc2.save())
    })

    it('should handle overlapping marks', () => {
      let doc : Automerge = create()
      let list = doc.set("_root", "list", TEXT)
      if (!list) throw new Error('should not be undefined')
      doc.splice(list, 0, 0, "the quick fox jumps over the lazy dog")
      doc.mark(list, "[0..37]", "bold" , true)
      doc.mark(list, "[4..19]", "itallic" , true)
      doc.mark(list, "[10..13]", "comment" , "foxes are my favorite animal!")
      let spans = doc.spans(list);
      assert.deepStrictEqual(spans,
        [
          [ [ 'bold', 'boolean', true ] ],
          'the ',
          [ [ 'bold', 'boolean', true ], [ 'itallic', 'boolean', true ] ],
          'quick ',
          [
            [ 'bold', 'boolean', true ],
            [ 'comment', 'str', 'foxes are my favorite animal!' ],
            [ 'itallic', 'boolean', true ]
          ],
          'fox',
          [ [ 'bold', 'boolean', true ], [ 'itallic', 'boolean', true ] ],
          ' jumps',
          [ [ 'bold', 'boolean', true ] ],
          ' over the lazy dog',
          [],
        ]
      )

      // mark sure encode decode can handle marks

      let all = doc.getChanges([])
      let decoded = all.map((c) => decodeChange(c))
      let encoded = decoded.map((c) => encodeChange(c))
      let doc2 = create();
      doc2.applyChanges(encoded)

      assert.deepStrictEqual(doc.spans(list) , doc2.spans(list))
      assert.deepStrictEqual(doc.save(), doc2.save())
    })

  })
  describe('sync', () => {
    it('should send a sync message implying no local data', () => {
      let doc = create()
      let s1 = initSyncState()
      let m1 = doc.generateSyncMessage(s1)
      const message: DecodedSyncMessage = decodeSyncMessage(m1)
      assert.deepStrictEqual(message.heads, [])
      assert.deepStrictEqual(message.need, [])
      assert.deepStrictEqual(message.have.length, 1)
      assert.deepStrictEqual(message.have[0].lastSync, [])
      assert.deepStrictEqual(message.have[0].bloom.byteLength, 0)
      assert.deepStrictEqual(message.changes, [])
    })

    it('should not reply if we have no data as well', () => {
        let n1 = create(), n2 = create()
        let s1 = initSyncState(), s2 = initSyncState()
        let m1 = n1.generateSyncMessage(s1)
        n2.receiveSyncMessage(s2, m1)
        let m2 = n2.generateSyncMessage(s2)
        assert.deepStrictEqual(m2, null)
    })

    it('repos with equal heads do not need a reply message', () => {
      let n1 = create(), n2 = create()
      let s1 = initSyncState(), s2 = initSyncState()

      // make two nodes with the same changes
      let list = n1.set("_root","n", LIST)
      if (!list) throw new Error('undefined')
      n1.commit("",0)
      for (let i = 0; i < 10; i++) {
        n1.insert(list,i,i)
        n1.commit("",0)
      }
      n2.applyChanges(n1.getChanges([]))
      assert.deepStrictEqual(n1.toJS(), n2.toJS())

      // generate a naive sync message
      let m1 = n1.generateSyncMessage(s1)
      assert.deepStrictEqual(s1.lastSentHeads, n1.getHeads())

      // heads are equal so this message should be null
      n2.receiveSyncMessage(s2, m1)
      let m2 = n2.generateSyncMessage(s2)
      assert.strictEqual(m2, null)
    })

    it('n1 should offer all changes to n2 when starting from nothing', () => {
      let n1 = create(), n2 = create()

      // make changes for n1 that n2 should request
      let list = n1.set("_root","n",LIST)
      if (!list) throw new Error('undefined')
      n1.commit("",0)
      for (let i = 0; i < 10; i++) {
        n1.insert(list, i, i)
        n1.commit("",0)
      }

      assert.notDeepStrictEqual(n1.toJS(), n2.toJS())
      sync(n1, n2)
      assert.deepStrictEqual(n1.toJS(), n2.toJS())
    })

    it('should sync peers where one has commits the other does not', () => {
      let n1 = create(), n2 = create()

      // make changes for n1 that n2 should request
      let list = n1.set("_root","n",LIST)
      if (!list) throw new Error('undefined')
      n1.commit("",0)
      for (let i = 0; i < 10; i++) {
        n1.insert(list,i,i)
        n1.commit("",0)
      }

      assert.notDeepStrictEqual(n1.toJS(), n2.toJS())
      sync(n1, n2)
      assert.deepStrictEqual(n1.toJS(), n2.toJS())
    })

    it('should work with prior sync state', () => {
      // create & synchronize two nodes
      let n1 = create(), n2 = create()
      let s1 = initSyncState(), s2 = initSyncState()

      for (let i = 0; i < 5; i++) {
        n1.set("_root","x",i)
        n1.commit("",0)
      }

      sync(n1, n2, s1, s2)

      // modify the first node further
      for (let i = 5; i < 10; i++) {
        n1.set("_root", "x", i)
        n1.commit("",0)
      }

      assert.notDeepStrictEqual(n1.toJS(), n2.toJS())
      sync(n1, n2, s1, s2)
      assert.deepStrictEqual(n1.toJS(), n2.toJS())
    })

    it('should not generate messages once synced', () => {
      // create & synchronize two nodes
      let n1 = create('abc123'), n2 = create('def456')
      let s1 = initSyncState(), s2 = initSyncState()

      let message, patch
      for (let i = 0; i < 5; i++) {
          n1.set("_root","x",i)
          n1.commit("",0)
      }
      for (let i = 0; i < 5; i++) {
          n2.set("_root","y",i)
          n2.commit("",0)
      }

      // n1 reports what it has
      message = n1.generateSyncMessage(s1)

      // n2 receives that message and sends changes along with what it has
      n2.receiveSyncMessage(s2, message)
      message = n2.generateSyncMessage(s2)
      assert.deepStrictEqual(decodeSyncMessage(message).changes.length, 5)
      //assert.deepStrictEqual(patch, null) // no changes arrived

      // n1 receives the changes and replies with the changes it now knows n2 needs
      n1.receiveSyncMessage(s1, message)
      message = n1.generateSyncMessage(s1)
      assert.deepStrictEqual(decodeSyncMessage(message).changes.length, 5)

      // n2 applies the changes and sends confirmation ending the exchange
      n2.receiveSyncMessage(s2, message)
      message = n2.generateSyncMessage(s2)

      // n1 receives the message and has nothing more to say
      n1.receiveSyncMessage(s1, message)
      message = n1.generateSyncMessage(s1)
      assert.deepStrictEqual(message, null)
      //assert.deepStrictEqual(patch, null) // no changes arrived

      // n2 also has nothing left to say
      message = n2.generateSyncMessage(s2)
      assert.deepStrictEqual(message, null)
    })

    it('should allow simultaneous messages during synchronization', () => {
      // create & synchronize two nodes
      let n1 = create('abc123'), n2 = create('def456')
      let s1 = initSyncState(), s2 = initSyncState()

      for (let i = 0; i < 5; i++) {
        n1.set("_root", "x",  i)
        n1.commit("",0)
      }
      for (let i = 0; i < 5; i++) {
          n2.set("_root","y", i)
          n2.commit("",0)
      }

      const head1 = n1.getHeads()[0], head2 = n2.getHeads()[0]

      // both sides report what they have but have no shared peer state
      let msg1to2, msg2to1
      msg1to2 = n1.generateSyncMessage(s1)
      msg2to1 = n2.generateSyncMessage(s2)
      assert.deepStrictEqual(decodeSyncMessage(msg1to2).changes.length, 0)
      assert.deepStrictEqual(decodeSyncMessage(msg1to2).have[0].lastSync.length, 0)
      assert.deepStrictEqual(decodeSyncMessage(msg2to1).changes.length, 0)
      assert.deepStrictEqual(decodeSyncMessage(msg2to1).have[0].lastSync.length, 0)

      // n1 and n2 receives that message and update sync state but make no patch
      n1.receiveSyncMessage(s1, msg2to1)
      n2.receiveSyncMessage(s2, msg1to2)

      // now both reply with their local changes the other lacks
      // (standard warning that 1% of the time this will result in a "need" message)
      msg1to2 = n1.generateSyncMessage(s1)
      assert.deepStrictEqual(decodeSyncMessage(msg1to2).changes.length, 5)
      msg2to1 = n2.generateSyncMessage(s2)
      assert.deepStrictEqual(decodeSyncMessage(msg2to1).changes.length, 5)

      // both should now apply the changes and update the frontend
      n1.receiveSyncMessage(s1, msg2to1)
      assert.deepStrictEqual(n1.getMissingDeps(), [])
      //assert.notDeepStrictEqual(patch1, null)
      assert.deepStrictEqual(n1.toJS(), {x: 4, y: 4})

      n2.receiveSyncMessage(s2, msg1to2)
      assert.deepStrictEqual(n2.getMissingDeps(), [])
      //assert.notDeepStrictEqual(patch2, null)
      assert.deepStrictEqual(n2.toJS(), {x: 4, y: 4})

      // The response acknowledges the changes received, and sends no further changes
      msg1to2 = n1.generateSyncMessage(s1)
      assert.deepStrictEqual(decodeSyncMessage(msg1to2).changes.length, 0)
      msg2to1 = n2.generateSyncMessage(s2)
      assert.deepStrictEqual(decodeSyncMessage(msg2to1).changes.length, 0)

      // After receiving acknowledgements, their shared heads should be equal
      n1.receiveSyncMessage(s1, msg2to1)
      n2.receiveSyncMessage(s2, msg1to2)
      assert.deepStrictEqual(s1.sharedHeads, [head1, head2].sort())
      assert.deepStrictEqual(s2.sharedHeads, [head1, head2].sort())
      //assert.deepStrictEqual(patch1, null)
      //assert.deepStrictEqual(patch2, null)

      // We're in sync, no more messages required
      msg1to2 = n1.generateSyncMessage(s1)
      msg2to1 = n2.generateSyncMessage(s2)
      assert.deepStrictEqual(msg1to2, null)
      assert.deepStrictEqual(msg2to1, null)

      // If we make one more change, and start another sync, its lastSync should be updated
      n1.set("_root","x",5)
      msg1to2 = n1.generateSyncMessage(s1)
      assert.deepStrictEqual(decodeSyncMessage(msg1to2).have[0].lastSync, [head1, head2].sort())
    })

    it('should assume sent changes were recieved until we hear otherwise', () => {
      let n1 = create('01234567'), n2 = create('89abcdef')
      let s1 = initSyncState(), s2 = initSyncState(), message = null

      let items = n1.set("_root", "items", LIST)
      if (!items) throw new Error('undefined')
      n1.commit("",0)

      sync(n1, n2, s1, s2)

      n1.push(items, "x")
      n1.commit("",0)
      message = n1.generateSyncMessage(s1)
      assert.deepStrictEqual(decodeSyncMessage(message).changes.length, 1)

      n1.push(items, "y")
      n1.commit("",0)
      message = n1.generateSyncMessage(s1)
      assert.deepStrictEqual(decodeSyncMessage(message).changes.length, 1)

      n1.push(items, "z")
      n1.commit("",0)

      message = n1.generateSyncMessage(s1)
      assert.deepStrictEqual(decodeSyncMessage(message).changes.length, 1)
    })

    it('should work regardless of who initiates the exchange', () => {
      // create & synchronize two nodes
      let n1 = create(), n2 = create()
      let s1 = initSyncState(), s2 = initSyncState()

      for (let i = 0; i < 5; i++) {
        n1.set("_root", "x", i)
        n1.commit("",0)
      }

      sync(n1, n2, s1, s2)

      // modify the first node further
      for (let i = 5; i < 10; i++) {
        n1.set("_root", "x", i)
        n1.commit("",0)
      }

      assert.notDeepStrictEqual(n1.toJS(), n2.toJS())
      sync(n1, n2, s1, s2)
      assert.deepStrictEqual(n1.toJS(), n2.toJS())
    })

    it('should work without prior sync state', () => {
      // Scenario:                                                            ,-- c10 <-- c11 <-- c12 <-- c13 <-- c14
      // c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8 <-- c9 <-+
      //                                                                      `-- c15 <-- c16 <-- c17
      // lastSync is undefined.

      // create two peers both with divergent commits
      let n1 = create('01234567'), n2 = create('89abcdef')
      let s1 = initSyncState(), s2 = initSyncState()

      for (let i = 0; i < 10; i++) {
        n1.set("_root","x",i)
        n1.commit("",0)
      }

      sync(n1, n2)

      for (let i = 10; i < 15; i++) {
        n1.set("_root","x",i)
        n1.commit("",0)
      }

      for (let i = 15; i < 18; i++) {
        n2.set("_root","x",i)
        n2.commit("",0)
      }

      assert.notDeepStrictEqual(n1.toJS(), n2.toJS())
      sync(n1, n2)
      assert.deepStrictEqual(n1.getHeads(), n2.getHeads())
      assert.deepStrictEqual(n1.toJS(), n2.toJS())
    })

    it('should work with prior sync state', () => {
      // Scenario:                                                            ,-- c10 <-- c11 <-- c12 <-- c13 <-- c14
      // c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8 <-- c9 <-+
      //                                                                      `-- c15 <-- c16 <-- c17
      // lastSync is c9.

      // create two peers both with divergent commits
      let n1 = create('01234567'), n2 = create('89abcdef')
      let s1 = initSyncState(), s2 = initSyncState()

      for (let i = 0; i < 10; i++) {
          n1.set("_root","x",i)
          n1.commit("",0)
      }

      sync(n1, n2, s1, s2)

      for (let i = 10; i < 15; i++) {
         n1.set("_root","x",i)
         n1.commit("",0)
      }
      for (let i = 15; i < 18; i++) {
         n2.set("_root","x",i)
         n2.commit("",0)
      }

      s1 = decodeSyncState(encodeSyncState(s1))
      s2 = decodeSyncState(encodeSyncState(s2))

      assert.notDeepStrictEqual(n1.toJS(), n2.toJS())
      sync(n1, n2, s1, s2)
      assert.deepStrictEqual(n1.getHeads(), n2.getHeads())
      assert.deepStrictEqual(n1.toJS(), n2.toJS())
    })

    it('should ensure non-empty state after sync', () => {
      let n1 = create('01234567'), n2 = create('89abcdef')
      let s1 = initSyncState(), s2 = initSyncState()

      for (let i = 0; i < 3; i++) {
        n1.set("_root","x",i)
        n1.commit("",0)
      }

      sync(n1, n2, s1, s2)

      assert.deepStrictEqual(s1.sharedHeads, n1.getHeads())
      assert.deepStrictEqual(s2.sharedHeads, n1.getHeads())
    })

    it('should re-sync after one node crashed with data loss', () => {
      // Scenario:     (r)                  (n2)                 (n1)
      // c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8
      // n2 has changes {c0, c1, c2}, n1's lastSync is c5, and n2's lastSync is c2.
      // we want to successfully sync (n1) with (r), even though (n1) believes it's talking to (n2)
      let n1 = create('01234567'), n2 = create('89abcdef')
      let s1 = initSyncState(), s2 = initSyncState()

      // n1 makes three changes, which we sync to n2
      for (let i = 0; i < 3; i++) {
        n1.set("_root","x",i)
        n1.commit("",0)
      }

      sync(n1, n2, s1, s2)

      // save a copy of n2 as "r" to simulate recovering from crash
      let r, rSyncState
      ;[r, rSyncState] = [n2.clone(), s2.clone()]

      // sync another few commits
      for (let i = 3; i < 6; i++) {
        n1.set("_root","x",i)
        n1.commit("",0)
      }

      sync(n1, n2, s1, s2)

      // everyone should be on the same page here
      assert.deepStrictEqual(n1.getHeads(), n2.getHeads())
      assert.deepStrictEqual(n1.toJS(), n2.toJS())

      // now make a few more changes, then attempt to sync the fully-up-to-date n1 with the confused r
      for (let i = 6; i < 9; i++) {
        n1.set("_root","x",i)
        n1.commit("",0)
      }

      s1 = decodeSyncState(encodeSyncState(s1))
      rSyncState = decodeSyncState(encodeSyncState(rSyncState))

      assert.notDeepStrictEqual(n1.getHeads(), r.getHeads())
      assert.notDeepStrictEqual(n1.toJS(), r.toJS())
      assert.deepStrictEqual(n1.toJS(), {x: 8})
      assert.deepStrictEqual(r.toJS(), {x: 2})
      sync(n1, r, s1, rSyncState)
      assert.deepStrictEqual(n1.getHeads(), r.getHeads())
      assert.deepStrictEqual(n1.toJS(), r.toJS())
    })

    it('should resync after one node experiences data loss without disconnecting', () => {
      let n1 = create('01234567'), n2 = create('89abcdef')
      let s1 = initSyncState(), s2 = initSyncState()

      // n1 makes three changes, which we sync to n2
      for (let i = 0; i < 3; i++) {
        n1.set("_root","x",i)
        n1.commit("",0)
      }

      sync(n1, n2, s1, s2)

      assert.deepStrictEqual(n1.getHeads(), n2.getHeads())
      assert.deepStrictEqual(n1.toJS(), n2.toJS())

      let n2AfterDataLoss = create('89abcdef')

      // "n2" now has no data, but n1 still thinks it does. Note we don't do
      // decodeSyncState(encodeSyncState(s1)) in order to simulate data loss without disconnecting
      sync(n1, n2AfterDataLoss, s1, initSyncState())
      assert.deepStrictEqual(n1.getHeads(), n2.getHeads())
      assert.deepStrictEqual(n1.toJS(), n2.toJS())
    })

    it('should handle changes concurrent to the last sync heads', () => {
      let n1 = create('01234567'), n2 = create('89abcdef'), n3 = create('fedcba98')
      let s12 = initSyncState(), s21 = initSyncState(), s23 = initSyncState(), s32 = initSyncState()

      // Change 1 is known to all three nodes
      //n1 = Automerge.change(n1, {time: 0}, doc => doc.x = 1)
      n1.set("_root","x",1); n1.commit("",0)

      sync(n1, n2, s12, s21)
      sync(n2, n3, s23, s32)

      // Change 2 is known to n1 and n2
      n1.set("_root","x",2); n1.commit("",0)

      sync(n1, n2, s12, s21)

      // Each of the three nodes makes one change (changes 3, 4, 5)
      n1.set("_root","x",3); n1.commit("",0)
      n2.set("_root","x",4); n2.commit("",0)
      n3.set("_root","x",5); n3.commit("",0)

      // Apply n3's latest change to n2. If running in Node, turn the Uint8Array into a Buffer, to
      // simulate transmission over a network (see https://github.com/automerge/automerge/pull/362)
      let change = n3.getLastLocalChange()
      //@ts-ignore
      if (typeof Buffer === 'function') change = Buffer.from(change)
      if (change === undefined) { throw new RangeError("last local change failed") }
      n2.applyChanges([change])

      // Now sync n1 and n2. n3's change is concurrent to n1 and n2's last sync heads
      sync(n1, n2, s12, s21)
      assert.deepStrictEqual(n1.getHeads(), n2.getHeads())
      assert.deepStrictEqual(n1.toJS(), n2.toJS())
    })

    it('should handle histories with lots of branching and merging', () => {
      let n1 = create('01234567'), n2 = create('89abcdef'), n3 = create('fedcba98')
      n1.set("_root","x",0); n1.commit("",0)
      n2.applyChanges([n1.getLastLocalChange()])
      n3.applyChanges([n1.getLastLocalChange()])
      n3.set("_root","x",1); n3.commit("",0)

      //        - n1c1 <------ n1c2 <------ n1c3 <-- etc. <-- n1c20 <------ n1c21
      //       /          \/           \/                              \/
      //      /           /\           /\                              /\
      // c0 <---- n2c1 <------ n2c2 <------ n2c3 <-- etc. <-- n2c20 <------ n2c21
      //      \                                                          /
      //       ---------------------------------------------- n3c1 <-----
      for (let i = 1; i < 20; i++) {
        n1.set("_root","n1",i); n1.commit("",0)
        n2.set("_root","n2",i); n2.commit("",0)
        const change1 = n1.getLastLocalChange()
        const change2 = n2.getLastLocalChange()
        n1.applyChanges([change2])
        n2.applyChanges([change1])
      }

      let s1 = initSyncState(), s2 = initSyncState()
      sync(n1, n2, s1, s2)

      // Having n3's last change concurrent to the last sync heads forces us into the slower code path
      n2.applyChanges([n3.getLastLocalChange()])
      n1.set("_root","n1","final"); n1.commit("",0)
      n2.set("_root","n2","final"); n2.commit("",0)

      sync(n1, n2, s1, s2)
      assert.deepStrictEqual(n1.getHeads(), n2.getHeads())
      assert.deepStrictEqual(n1.toJS(), n2.toJS())
    })

    it('should handle a false-positive head', () => {
      // Scenario:                                                            ,-- n1
      // c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8 <-- c9 <-+
      //                                                                      `-- n2
      // where n2 is a false positive in the Bloom filter containing {n1}.
      // lastSync is c9.
      let n1 = create('01234567'), n2 = create('89abcdef')
      let s1 = initSyncState(), s2 = initSyncState()

      for (let i = 0; i < 10; i++) {
        n1.set("_root","x",i); n1.commit("",0)
      }

      sync(n1, n2, s1, s2)
      for (let i = 1; ; i++) { // search for false positive; see comment above
        const n1up = n1.clone('01234567');
        n1up.set("_root","x",`${i} @ n1`); n1up.commit("",0)
        const n2up = n2.clone('89abcdef');
        n2up.set("_root","x",`${i} @ n2`); n2up.commit("",0)
        if (new BloomFilter(n1up.getHeads()).containsHash(n2up.getHeads()[0])) {
          n1.free(); n2.free()
          n1 = n1up; n2 = n2up; break
        }
      }
      const allHeads = [...n1.getHeads(), ...n2.getHeads()].sort()
      s1 = decodeSyncState(encodeSyncState(s1))
      s2 = decodeSyncState(encodeSyncState(s2))
      sync(n1, n2, s1, s2)
      assert.deepStrictEqual(n1.getHeads(), allHeads)
      assert.deepStrictEqual(n2.getHeads(), allHeads)
    })


    describe('with a false-positive dependency', () => {
      let n1: Automerge, n2: Automerge, s1: SyncState, s2: SyncState, n1hash2: Hash, n2hash2: Hash

      beforeEach(() => {
        // Scenario:                                                            ,-- n1c1 <-- n1c2
        // c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8 <-- c9 <-+
        //                                                                      `-- n2c1 <-- n2c2
        // where n2c1 is a false positive in the Bloom filter containing {n1c1, n1c2}.
        // lastSync is c9.
        n1 = create('01234567')
        n2 = create('89abcdef')
        s1 = initSyncState()
        s2 = initSyncState()
        for (let i = 0; i < 10; i++) {
          n1.set("_root","x",i); n1.commit("",0)
        }
        sync(n1, n2, s1, s2)

        let n1hash1, n2hash1
        for (let i = 29; ; i++) { // search for false positive; see comment above
          const n1us1 = n1.clone('01234567')
          n1us1.set("_root","x",`${i} @ n1`); n1us1.commit("",0)

          const n2us1 = n2.clone('89abcdef')
          n2us1.set("_root","x",`${i} @ n1`); n2us1.commit("",0)

          n1hash1 = n1us1.getHeads()[0]; n2hash1 = n2us1.getHeads()[0]

          const n1us2 = n1us1.clone();
          n1us2.set("_root","x",`final @ n1`); n1us2.commit("",0)

          const n2us2 = n2us1.clone();
          n2us2.set("_root","x",`final @ n2`); n2us2.commit("",0)

          n1hash2 = n1us2.getHeads()[0]; n2hash2 = n2us2.getHeads()[0]
          if (new BloomFilter([n1hash1, n1hash2]).containsHash(n2hash1)) {
            n1.free(); n2.free()
            n1 = n1us2; n2 = n2us2; break
          }
        }
      })

      it('should sync two nodes without connection reset', () => {
        sync(n1, n2, s1, s2)
        assert.deepStrictEqual(n1.getHeads(), [n1hash2, n2hash2].sort())
        assert.deepStrictEqual(n2.getHeads(), [n1hash2, n2hash2].sort())
      })

      it('should sync two nodes with connection reset', () => {
        s1 = decodeSyncState(encodeSyncState(s1))
        s2 = decodeSyncState(encodeSyncState(s2))
        sync(n1, n2, s1, s2)
        assert.deepStrictEqual(n1.getHeads(), [n1hash2, n2hash2].sort())
        assert.deepStrictEqual(n2.getHeads(), [n1hash2, n2hash2].sort())
      })

      it('should sync three nodes', () => {
        s1 = decodeSyncState(encodeSyncState(s1))
        s2 = decodeSyncState(encodeSyncState(s2))

        // First n1 and n2 exchange Bloom filters
        let m1, m2
        m1 = n1.generateSyncMessage(s1)
        m2 = n2.generateSyncMessage(s2)
        n1.receiveSyncMessage(s1, m2)
        n2.receiveSyncMessage(s2, m1)

        // Then n1 and n2 send each other their changes, except for the false positive
        m1 = n1.generateSyncMessage(s1)
        m2 = n2.generateSyncMessage(s2)
        n1.receiveSyncMessage(s1, m2)
        n2.receiveSyncMessage(s2, m1)
        assert.strictEqual(decodeSyncMessage(m1).changes.length, 2) // n1c1 and n1c2
        assert.strictEqual(decodeSyncMessage(m2).changes.length, 1) // only n2c2; change n2c1 is not sent

        // n3 is a node that doesn't have the missing change. Nevertheless n1 is going to ask n3 for it
        let n3 = create('fedcba98'), s13 = initSyncState(), s31 = initSyncState()
        sync(n1, n3, s13, s31)
        assert.deepStrictEqual(n1.getHeads(), [n1hash2])
        assert.deepStrictEqual(n3.getHeads(), [n1hash2])
      })
    })

    it('should not require an additional request when a false-positive depends on a true-negative', () => {
      // Scenario:                         ,-- n1c1 <-- n1c2 <-- n1c3
      // c0 <-- c1 <-- c2 <-- c3 <-- c4 <-+
      //                                   `-- n2c1 <-- n2c2 <-- n2c3
      // where n2c2 is a false positive in the Bloom filter containing {n1c1, n1c2, n1c3}.
      // lastSync is c4.
      let n1 = create('01234567'), n2 = create('89abcdef')
      let s1 = initSyncState(), s2 = initSyncState()
      let n1hash3, n2hash3

      for (let i = 0; i < 5; i++) {
        n1.set("_root","x",i); n1.commit("",0)
      }
      sync(n1, n2, s1, s2)
      for (let i = 86; ; i++) { // search for false positive; see comment above
        const n1us1 = n1.clone('01234567')
        n1us1.set("_root","x",`${i} @ n1`); n1us1.commit("",0)

        const n2us1 = n2.clone('89abcdef')
        n2us1.set("_root","x",`${i} @ n2`); n2us1.commit("",0)

        //const n1us1 = Automerge.change(Automerge.clone(n1, {actorId: '01234567'}), {time: 0}, doc => doc.x = `${i} @ n1`)
        //const n2us1 = Automerge.change(Automerge.clone(n2, {actorId: '89abcdef'}), {time: 0}, doc => doc.x = `${i} @ n2`)
        const n1hash1 = n1us1.getHeads()[0]

        const n1us2 = n1us1.clone()
        n1us2.set("_root","x",`${i + 1} @ n1`); n1us2.commit("",0)

        const n2us2 = n2us1.clone()
        n2us2.set("_root","x",`${i + 1} @ n2`); n2us2.commit("",0)

        const n1hash2 = n1us2.getHeads()[0], n2hash2 = n2us2.getHeads()[0]

        const n1us3 = n1us2.clone()
        n1us3.set("_root","x",`final @ n1`); n1us3.commit("",0)

        const n2us3 = n2us2.clone()
        n2us3.set("_root","x",`final @ n2`); n2us3.commit("",0)

        n1hash3 = n1us3.getHeads()[0]; n2hash3 = n2us3.getHeads()[0]

        if (new BloomFilter([n1hash1, n1hash2, n1hash3]).containsHash(n2hash2)) {
          n1.free(); n2.free();
          n1 = n1us3; n2 = n2us3; break
        }
      }
      const bothHeads = [n1hash3, n2hash3].sort()
      s1 = decodeSyncState(encodeSyncState(s1))
      s2 = decodeSyncState(encodeSyncState(s2))
      sync(n1, n2, s1, s2)
      assert.deepStrictEqual(n1.getHeads(), bothHeads)
      assert.deepStrictEqual(n2.getHeads(), bothHeads)
    })

    it('should handle chains of false-positives', () => {
      // Scenario:                         ,-- c5
      // c0 <-- c1 <-- c2 <-- c3 <-- c4 <-+
      //                                   `-- n2c1 <-- n2c2 <-- n2c3
      // where n2c1 and n2c2 are both false positives in the Bloom filter containing {c5}.
      // lastSync is c4.
      let n1 = create('01234567'), n2 = create('89abcdef')
      let s1 = initSyncState(), s2 = initSyncState()

      for (let i = 0; i < 5; i++) {
        n1.set("_root","x",i); n1.commit("",0)
      }

      sync(n1, n2, s1, s2)

      n1.set("_root","x",5); n1.commit("",0)

      for (let i = 2; ; i++) { // search for false positive; see comment above
        const n2us1 = n2.clone('89abcdef')
        n2us1.set("_root","x",`${i} @ n2`); n2us1.commit("",0)
        if (new BloomFilter(n1.getHeads()).containsHash(n2us1.getHeads()[0])) {
          n2 = n2us1; break
        }
      }
      for (let i = 141; ; i++) { // search for false positive; see comment above
        const n2us2 = n2.clone('89abcdef')
        n2us2.set("_root","x",`${i} again`); n2us2.commit("",0)
        if (new BloomFilter(n1.getHeads()).containsHash(n2us2.getHeads()[0])) {
          n2 = n2us2; break
        }
      }
      n2.set("_root","x",`final @ n2`); n2.commit("",0)

      const allHeads = [...n1.getHeads(), ...n2.getHeads()].sort()
      s1 = decodeSyncState(encodeSyncState(s1))
      s2 = decodeSyncState(encodeSyncState(s2))
      sync(n1, n2, s1, s2)
      assert.deepStrictEqual(n1.getHeads(), allHeads)
      assert.deepStrictEqual(n2.getHeads(), allHeads)
    })

    it('should allow the false-positive hash to be explicitly requested', () => {
      // Scenario:                                                            ,-- n1
      // c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8 <-- c9 <-+
      //                                                                      `-- n2
      // where n2 causes a false positive in the Bloom filter containing {n1}.
      let n1 = create('01234567'), n2 = create('89abcdef')
      let s1 = initSyncState(), s2 = initSyncState()
      let message

      for (let i = 0; i < 10; i++) {
        n1.set("_root","x",i); n1.commit("",0)
      }

      sync(n1, n2, s1, s2)

      s1 = decodeSyncState(encodeSyncState(s1))
      s2 = decodeSyncState(encodeSyncState(s2))

      for (let i = 1; ; i++) { // brute-force search for false positive; see comment above
        const n1up = n1.clone('01234567'); n1up.set("_root","x",`${i} @ n1`); n1up.commit("",0)
        const n2up = n1.clone('89abcdef'); n2up.set("_root","x",`${i} @ n2`); n2up.commit("",0)

        // check if the bloom filter on n2 will believe n1 already has a particular hash
        // this will mean n2 won't offer that data to n2 by receiving a sync message from n1
        if (new BloomFilter(n1up.getHeads()).containsHash(n2up.getHeads()[0])) {
          n1 = n1up; n2 = n2up; break
        }
      }

      // n1 creates a sync message for n2 with an ill-fated bloom
      message = n1.generateSyncMessage(s1)
      assert.strictEqual(decodeSyncMessage(message).changes.length, 0)

      // n2 receives it and DOESN'T send a change back
      n2.receiveSyncMessage(s2, message)
      message = n2.generateSyncMessage(s2)
      assert.strictEqual(decodeSyncMessage(message).changes.length, 0)

      // n1 should now realize it's missing that change and request it explicitly
      n1.receiveSyncMessage(s1, message)
      message = n1.generateSyncMessage(s1)
      assert.deepStrictEqual(decodeSyncMessage(message).need, n2.getHeads())

      // n2 should fulfill that request
      n2.receiveSyncMessage(s2, message)
      message = n2.generateSyncMessage(s2)
      assert.strictEqual(decodeSyncMessage(message).changes.length, 1)

      // n1 should apply the change and the two should now be in sync
      n1.receiveSyncMessage(s1, message)
      assert.deepStrictEqual(n1.getHeads(), n2.getHeads())
    })

    describe('protocol features', () => {
      it('should allow multiple Bloom filters', () => {
        // Scenario:           ,-- n1c1 <-- n1c2 <-- n1c3
        // c0 <-- c1 <-- c2 <-+--- n2c1 <-- n2c2 <-- n2c3
        //                     `-- n3c1 <-- n3c2 <-- n3c3
        // n1 has {c0, c1, c2, n1c1, n1c2, n1c3, n2c1, n2c2};
        // n2 has {c0, c1, c2, n1c1, n1c2, n2c1, n2c2, n2c3};
        // n3 has {c0, c1, c2, n3c1, n3c2, n3c3}.
        let n1 = create('01234567'), n2 = create('89abcdef'), n3 = create('76543210')
        let s13 = initSyncState(), s12 = initSyncState(), s21 = initSyncState()
        let s32 = initSyncState(), s31 = initSyncState(), s23 = initSyncState()
        let message1, message2, message3

        for (let i = 0; i < 3; i++) {
          n1.set("_root","x",i); n1.commit("",0)
        }

        // sync all 3 nodes
        sync(n1, n2, s12, s21) // eslint-disable-line no-unused-vars -- kept for consistency
        sync(n1, n3, s13, s31)
        sync(n3, n2, s32, s23)
        for (let i = 0; i < 2; i++) {
          n1.set("_root","x",`${i} @ n1`); n1.commit("",0)
        }
        for (let i = 0; i < 2; i++) {
          n2.set("_root","x",`${i} @ n2`); n2.commit("",0)
        }
        n1.applyChanges(n2.getChanges([]))
        n2.applyChanges(n1.getChanges([]))
        n1.set("_root","x",`3 @ n1`); n1.commit("",0)
        n2.set("_root","x",`3 @ n2`); n2.commit("",0)

        for (let i = 0; i < 3; i++) {
          n3.set("_root","x",`${i} @ n3`); n3.commit("",0)
        }
        const n1c3 = n1.getHeads()[0], n2c3 = n2.getHeads()[0], n3c3 = n3.getHeads()[0]
        s13 = decodeSyncState(encodeSyncState(s13))
        s31 = decodeSyncState(encodeSyncState(s31))
        s23 = decodeSyncState(encodeSyncState(s23))
        s32 = decodeSyncState(encodeSyncState(s32))


        // Now n3 concurrently syncs with n1 and n2. Doing this naively would result in n3 receiving
        // changes {n1c1, n1c2, n2c1, n2c2} twice (those are the changes that both n1 and n2 have, but
        // that n3 does not have). We want to prevent this duplication.
        message1 = n1.generateSyncMessage(s13) // message from n1 to n3
        assert.strictEqual(decodeSyncMessage(message1).changes.length, 0)
        n3.receiveSyncMessage(s31, message1)
        message3 = n3.generateSyncMessage(s31) // message from n3 to n1
        assert.strictEqual(decodeSyncMessage(message3).changes.length, 3) // {n3c1, n3c2, n3c3}
        n1.receiveSyncMessage(s13, message3)

        // Copy the Bloom filter received from n1 into the message sent from n3 to n2. This Bloom
        // filter indicates what changes n3 is going to receive from n1.
        message3 = n3.generateSyncMessage(s32) // message from n3 to n2
        const modifiedMessage = decodeSyncMessage(message3)
        modifiedMessage.have.push(decodeSyncMessage(message1).have[0])
        assert.strictEqual(modifiedMessage.changes.length, 0)
        n2.receiveSyncMessage(s23, encodeSyncMessage(modifiedMessage))

        // n2 replies to n3, sending only n2c3 (the one change that n2 has but n1 doesn't)
        message2 = n2.generateSyncMessage(s23)
        assert.strictEqual(decodeSyncMessage(message2).changes.length, 1) // {n2c3}
        n3.receiveSyncMessage(s32, message2)

        // n1 replies to n3
        message1 = n1.generateSyncMessage(s13)
        assert.strictEqual(decodeSyncMessage(message1).changes.length, 5) // {n1c1, n1c2, n1c3, n2c1, n2c2}
        n3.receiveSyncMessage(s31, message1)
        assert.deepStrictEqual(n3.getHeads(), [n1c3, n2c3, n3c3].sort())
      })

      it('should allow any change to be requested', () => {
        let n1 = create('01234567'), n2 = create('89abcdef')
        let s1 = initSyncState(), s2 = initSyncState()
        let message = null

        for (let i = 0; i < 3; i++) {
          n1.set("_root","x",i); n1.commit("",0)
        }

        const lastSync = n1.getHeads()

        for (let i = 3; i < 6; i++) {
          n1.set("_root","x",i); n1.commit("",0)
        }

        sync(n1, n2, s1, s2)
        s1.lastSentHeads = [] // force generateSyncMessage to return a message even though nothing changed
        message = n1.generateSyncMessage(s1)
        const modMsg = decodeSyncMessage(message)
        modMsg.need = lastSync // re-request change 2
        n2.receiveSyncMessage(s2, encodeSyncMessage(modMsg))
        message = n2.generateSyncMessage(s2)
        assert.strictEqual(decodeSyncMessage(message).changes.length, 1)
        assert.strictEqual(decodeChange(decodeSyncMessage(message).changes[0]).hash, lastSync[0])
      })

      it('should ignore requests for a nonexistent change', () => {
        let n1 = create('01234567'), n2 = create('89abcdef')
        let s1 = initSyncState(), s2 = initSyncState()
        let message = null

        for (let i = 0; i < 3; i++) {
          n1.set("_root","x",i); n1.commit("",0)
        }

        n2.applyChanges(n1.getChanges([]))
        message = n1.generateSyncMessage(s1)
        message = decodeSyncMessage(message)
        message.need = ['0000000000000000000000000000000000000000000000000000000000000000']
        message = encodeSyncMessage(message)
        n2.receiveSyncMessage(s2, message)
        message = n2.generateSyncMessage(s2)
        assert.strictEqual(message, null)
      })

      it('should allow a subset of changes to be sent', () => {
        //       ,-- c1 <-- c2
        // c0 <-+
        //       `-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8
        let n1 = create('01234567'), n2 = create('89abcdef'), n3 = create('76543210')
        let s1 = initSyncState(), s2 = initSyncState()
        let msg, decodedMsg

        n1.set("_root","x",0); n1.commit("",0)
        n3.applyChanges(n3.getChangesAdded(n1)) // merge()
        for (let i = 1; i <= 2; i++) {
          n1.set("_root","x",i); n1.commit("",0)
        }
        for (let i = 3; i <= 4; i++) {
          n3.set("_root","x",i); n3.commit("",0)
        }
        const c2 = n1.getHeads()[0], c4 = n3.getHeads()[0]
        n2.applyChanges(n2.getChangesAdded(n3)) // merge()

        // Sync n1 and n2, so their shared heads are {c2, c4}
        sync(n1, n2, s1, s2)
        s1 = decodeSyncState(encodeSyncState(s1))
        s2 = decodeSyncState(encodeSyncState(s2))
        assert.deepStrictEqual(s1.sharedHeads, [c2, c4].sort())
        assert.deepStrictEqual(s2.sharedHeads, [c2, c4].sort())

        // n2 and n3 apply {c5, c6, c7, c8}
        n3.set("_root","x",5); n3.commit("",0)
        const change5 = n3.getLastLocalChange()
        n3.set("_root","x",6); n3.commit("",0)
        const change6 = n3.getLastLocalChange(), c6 = n3.getHeads()[0]
        for (let i = 7; i <= 8; i++) {
          n3.set("_root","x",i); n3.commit("",0)
        }
        const c8 = n3.getHeads()[0]
        n2.applyChanges(n2.getChangesAdded(n3)) // merge()

        // Now n1 initiates a sync with n2, and n2 replies with {c5, c6}. n2 does not send {c7, c8}
        msg = n1.generateSyncMessage(s1)
        n2.receiveSyncMessage(s2, msg)
        msg = n2.generateSyncMessage(s2)
        decodedMsg = decodeSyncMessage(msg)
        decodedMsg.changes = [change5, change6]
        msg = encodeSyncMessage(decodedMsg)
        const sentHashes: any = {}

        sentHashes[decodeChange(change5).hash] = true
        sentHashes[decodeChange(change6).hash] = true
        s2.sentHashes = sentHashes
        n1.receiveSyncMessage(s1, msg)
        assert.deepStrictEqual(s1.sharedHeads, [c2, c6].sort())

        // n1 replies, confirming the receipt of {c5, c6} and requesting the remaining changes
        msg = n1.generateSyncMessage(s1)
        n2.receiveSyncMessage(s2, msg)
        assert.deepStrictEqual(decodeSyncMessage(msg).need, [c8])
        assert.deepStrictEqual(decodeSyncMessage(msg).have[0].lastSync, [c2, c6].sort())
        assert.deepStrictEqual(s1.sharedHeads, [c2, c6].sort())
        assert.deepStrictEqual(s2.sharedHeads, [c2, c6].sort())

        // n2 sends the remaining changes {c7, c8}
        msg = n2.generateSyncMessage(s2)
        n1.receiveSyncMessage(s1, msg)
        assert.strictEqual(decodeSyncMessage(msg).changes.length, 2)
        assert.deepStrictEqual(s1.sharedHeads, [c2, c8].sort())
      })
    })
  })
})
