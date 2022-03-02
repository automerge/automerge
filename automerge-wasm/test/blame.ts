import { describe, it } from 'mocha';
//@ts-ignore
import assert from 'assert'
//@ts-ignore
import { BloomFilter } from './helpers/sync'
import { create, loadDoc, SyncState, Automerge, encodeChange, decodeChange, initSyncState, decodeSyncMessage, decodeSyncState, encodeSyncState, encodeSyncMessage } from '..'
import { DecodedSyncMessage, Hash } from '..'

describe('Automerge', () => {
  describe('blame', () => {
    it.only('should be able to blame text segments on change sets', () => {
      let doc1 = create()
      let text = doc1.make("_root", "notes","hello little world")
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
      let blame = doc1.blame(text, h1, [h2, h3])

      assert.deepEqual(blame, [
        { add: [ { start: 11, end: 15 } ], del: [ { pos: 15, val: ' little' } ] },
        { add: [ { start: 0,  end: 6  } ], del: [] }
      ])
    })

    it.only('should be able to hand complex blame change sets', () => {
      let doc1 = create("aaaa")
      let text = doc1.make("_root", "notes","AAAAAA")
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

      assert.deepEqual(doc1.blame(text, h1, [h2]), [
        { add: [ {start:0, end: 4}, { start: 6, end: 8 } ], del: [ { pos: 4, val: 'AAAA' } ] },
      ])

      doc1.merge(doc3)

      assert.deepEqual(doc1.text(text), "BBBBCCACBB")

      // with tombstones its 
      // BBBB.C..C.AC.BB
      assert.deepEqual(doc1.blame(text, h1, [h2,h3]), [  
        { add: [ {start:0, end: 4}, { start: 8, end: 10 } ], del: [ { pos: 4, val: 'A' }, { pos: 5, val: 'AA' }, { pos: 6, val: 'A' } ] },
        { add: [ {start:4, end: 6}, { start: 7, end: 8 } ], del: [ { pos: 5, val: 'A' }, { pos: 6, val: 'A' }, { pos: 8, val: 'A' } ] }
      ])
    })
  })
})
