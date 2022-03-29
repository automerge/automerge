import { describe, it } from 'mocha';
//@ts-ignore
import assert from 'assert'
//@ts-ignore
import { BloomFilter } from './helpers/sync'
import { create, loadDoc, SyncState, Automerge, encodeChange, decodeChange, initSyncState, decodeSyncMessage, decodeSyncState, encodeSyncState, encodeSyncMessage } from '..'
import { DecodedSyncMessage, Hash } from '..'

describe('Automerge', () => {
  describe('attribute', () => {
    it('should be able to attribute text segments on change sets', () => {
      let doc1 = create()
      let text = doc1.set_object("_root", "notes","hello little world")
      let h1 = doc1.getHeads();

      let doc2 = doc1.fork();
      doc2.splice(text, 5, 7, " big");
      doc2.text(text)
      let h2 = doc2.getHeads();
      assert.deepEqual(doc2.text(text), "hello big world")

      let doc3 = doc1.fork();
      doc3.splice(text, 0, 0, "Well, ");
      let h3 = doc3.getHeads();
      assert.deepEqual(doc3.text(text), "Well, hello little world")

      doc1.merge(doc2)
      doc1.merge(doc3)
      assert.deepEqual(doc1.text(text), "Well, hello big world")
      let attribute = doc1.attribute(text, h1, [h2, h3])

      assert.deepEqual(attribute, [
        { add: [ { start: 11, end: 15 } ], del: [ { pos: 15, val: ' little' } ] },
        { add: [ { start: 0,  end: 6  } ], del: [] }
      ])
    })

    it('should be able to hand complex attribute change sets', () => {
      let doc1 = create("aaaa")
      let text = doc1.set_object("_root", "notes","AAAAAA")
      let h1 = doc1.getHeads();

      let doc2 = doc1.fork("bbbb");
      doc2.splice(text, 0, 2, "BB");
      doc2.commit()
      doc2.splice(text, 2, 2, "BB");
      doc2.commit()
      doc2.splice(text, 6, 0, "BB");
      doc2.commit()
      let h2 = doc2.getHeads();
      assert.deepEqual(doc2.text(text), "BBBBAABB")

      let doc3 = doc1.fork("cccc");
      doc3.splice(text, 1, 1, "C");
      doc3.commit()
      doc3.splice(text, 3, 1, "C");
      doc3.commit()
      doc3.splice(text, 5, 1, "C");
      doc3.commit()
      let h3 = doc3.getHeads();
      // with tombstones its 
      // AC.AC.AC.
      assert.deepEqual(doc3.text(text), "ACACAC")

      doc1.merge(doc2)

      assert.deepEqual(doc1.attribute(text, h1, [h2]), [
        { add: [ {start:0, end: 4}, { start: 6, end: 8 } ], del: [ { pos: 4, val: 'AAAA' } ] },
      ])

      doc1.merge(doc3)

      assert.deepEqual(doc1.text(text), "BBBBCCACBB")

      // with tombstones its 
      // BBBB.C..C.AC.BB
      assert.deepEqual(doc1.attribute(text, h1, [h2,h3]), [  
        { add: [ {start:0, end: 4}, { start: 8, end: 10 } ], del: [ { pos: 4, val: 'A' }, { pos: 5, val: 'AA' }, { pos: 6, val: 'A' } ] },
        { add: [ {start:4, end: 6}, { start: 7, end: 8 } ], del: [ { pos: 5, val: 'A' }, { pos: 6, val: 'A' }, { pos: 8, val: 'A' } ] }
      ])
    })

    it('should not include attribution of text that is inserted and deleted only within change sets', () => {
      let doc1 = create()
      let text = doc1.set_object("_root", "notes","hello little world")
      let h1 = doc1.getHeads();

      let doc2 = doc1.fork();
      doc2.splice(text, 5, 7, " big");
      doc2.splice(text, 9, 0, " bad");
      doc2.splice(text, 9, 4)
      doc2.text(text)
      let h2 = doc2.getHeads();
      assert.deepEqual(doc2.text(text), "hello big world")

      let doc3 = doc1.fork();
      doc3.splice(text, 0, 0, "Well, HI THERE");
      doc3.splice(text, 6, 8, "")
      let h3 = doc3.getHeads();
      assert.deepEqual(doc3.text(text), "Well, hello little world")

      doc1.merge(doc2)
      doc1.merge(doc3)
      assert.deepEqual(doc1.text(text), "Well, hello big world")
      let attribute = doc1.attribute(text, h1, [h2, h3])

      assert.deepEqual(attribute, [
        { add: [ { start: 11, end: 15 } ], del: [ { pos: 15, val: ' little' } ] },
        { add: [ { start: 0,  end: 6  } ], del: [] }
      ])
    })

  })
  describe('attribute2', () => {
    it('should be able to attribute text segments on change sets', () => {
      let doc1 = create("aaaa")
      let text = doc1.set_object("_root", "notes","hello little world")
      let h1 = doc1.getHeads();

      let doc2 = doc1.fork("bbbb");
      doc2.splice(text, 5, 7, " big");
      doc2.text(text)
      let h2 = doc2.getHeads();
      assert.deepEqual(doc2.text(text), "hello big world")

      let doc3 = doc1.fork("cccc");
      doc3.splice(text, 0, 0, "Well, ");
      let doc4 = doc3.fork("dddd")
      doc4.splice(text, 0, 0, "Gee, ");
      let h3 = doc4.getHeads();
      assert.deepEqual(doc4.text(text), "Gee, Well, hello little world")

      doc1.merge(doc2)
      doc1.merge(doc4)
      assert.deepEqual(doc1.text(text), "Gee, Well, hello big world")
      let attribute = doc1.attribute2(text, h1, [h2, h3])

      assert.deepEqual(attribute, [
        { add: [ { actor: "bbbb", start: 16, end: 20 } ], del: [ { actor: "bbbb", pos: 20, val: ' little' } ] },
        { add: [ { actor: "dddd", start:0, end: 5 }, { actor: "cccc", start: 5,  end: 11  } ], del: [] }
      ])
    })

    it('should not include attribution of text that is inserted and deleted only within change sets', () => {
      let doc1 = create("aaaa")
      let text = doc1.set_object("_root", "notes","hello little world")
      let h1 = doc1.getHeads();

      let doc2 = doc1.fork("bbbb");
      doc2.splice(text, 5, 7, " big");
      doc2.splice(text, 9, 0, " bad");
      doc2.splice(text, 9, 4)
      doc2.text(text)
      let h2 = doc2.getHeads();
      assert.deepEqual(doc2.text(text), "hello big world")

      let doc3 = doc1.fork("cccc");
      doc3.splice(text, 0, 0, "Well, HI THERE");
      doc3.splice(text, 6, 8, "")
      let h3 = doc3.getHeads();
      assert.deepEqual(doc3.text(text), "Well, hello little world")

      doc1.merge(doc2)
      doc1.merge(doc3)
      assert.deepEqual(doc1.text(text), "Well, hello big world")
      let attribute = doc1.attribute2(text, h1, [h2, h3])

      assert.deepEqual(attribute, [
        { add: [ { start: 11, end: 15, actor: "bbbb" } ], del: [ { pos: 15, val: ' little', actor: "bbbb" } ] },
        { add: [ { start: 0,  end: 6,  actor: "cccc" } ], del: [] }
      ])

      let h4 = doc1.getHeads()

      doc3.splice(text, 24, 0, "!!!")
      doc1.merge(doc3)

      let h5 = doc1.getHeads()

      assert.deepEqual(doc1.text(text), "Well, hello big world!!!")
      attribute = doc1.attribute2(text, h4, [h5])

      assert.deepEqual(attribute, [
        { add: [ { start: 21, end: 24, actor: "cccc" } ], del: [] },
        { add: [], del: [] }
      ])
    })
  })
})
