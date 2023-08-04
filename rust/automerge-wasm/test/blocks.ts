import { describe, it } from 'mocha';
//@ts-ignore
import assert from 'assert'
//@ts-ignore
import { create, load, Automerge, encodeChange, decodeChange } from '..'
import { v4 as uuid } from "uuid"


let util = require('util')

describe('Automerge', () => {
  describe('blocks', () => {
    it('can split a block', () => {
      let doc = create({ actor: "aabbcc" })
      let list = doc.putObject("_root", "list", "🐻🐻🐻bbbccc")
      doc.updateDiffCursor();
      let doc2 = doc.fork();
      let blockId = doc.splitBlock(list, 6, { name: "li", parents: ["ul"] });
      let patches1 = doc.diffIncremental();
      assert.deepStrictEqual(doc.text(list), "🐻🐻🐻\ufffcbbbccc")
      assert.deepStrictEqual(doc.length(list), 13)
      doc.joinBlock(blockId);
      let patches2 = doc.diffIncremental();
      assert.deepStrictEqual(patches1, [
        { action: 'insert', path: ['list',6], values: [{}] },
        { action: 'put', path: [ 'list', 6, 'name' ], value: 'li' },
        { action: 'put', path: [ 'list', 6, 'parents' ], value: [] },
        { action: 'insert', path: [ 'list', 6, 'parents', 0 ], values: [ 'ul' ] }
      ]);
      assert.deepStrictEqual(patches2, [
        { action: 'del', path: ['list',6] }
      ]);
      assert.deepStrictEqual(doc.text(list), "🐻🐻🐻bbbccc")
      assert.deepStrictEqual(doc.length(list), 12)

      doc2.updateDiffCursor();
      doc2.merge(doc);
      let patches3 = doc2.diffIncremental();
      assert.deepStrictEqual(patches3, []);
    })
    it('patches correctly reference blocks', () => {
      let doc = create({ actor: "aabbcc" })
      let text = doc.putObject("_root", "text", "aaabbbccc")
      let starterHeads = doc.getHeads();
      doc.updateDiffCursor();
      let doc2 = doc.fork();
      let block = { name: "li", parents: ["ul"] };
      let blockId = doc.splitBlock(text, 3, block);
      //doc.updateBlock(text, blockId, "div", ["block","pre"]);
      let blockHeads = doc.getHeads()
      doc.joinBlock(blockId);
      doc.commit();
      let patches = doc.diffIncremental();
      assert.deepStrictEqual(patches,[]);
      assert.deepStrictEqual(doc.text(text), "aaabbbccc")
      assert.deepStrictEqual(doc.length(text), 9)

      doc2.updateDiffCursor();
      doc2.merge(doc);
      let patches2 = doc2.diffIncremental();
      assert.deepStrictEqual(patches2,[]); // insert and delete
      let doc3 = doc.fork(undefined,blockHeads);
      let patches3A = doc3.diff([],doc3.getHeads());
      let patches3B = doc.diff([],blockHeads);
      let patches3C = doc.diff(blockHeads, starterHeads);
      assert.deepStrictEqual(patches3A, [
        { action: 'put', path: [ 'text' ], value: '' },
        { action: 'splice', path: [ 'text', 0 ], value: 'aaa' },
        { action: 'insert', path: [ 'text', 3 ], values: [{}] },
        { action: 'splice', path: [ 'text', 4 ], value: 'bbbccc', block },
        { action: 'put', path: [ 'text', 3, 'name' ], value: 'li' },
        { action: 'put', path: [ 'text', 3, 'parents' ], value: [] },
        { action: 'insert', path: [ 'text', 3, 'parents', 0 ], values: [ 'ul' ] }
      ]);
      assert.deepStrictEqual(patches3A, patches3B);
      assert.deepStrictEqual(patches3C, [
        { action: "del", path: [ "text", 3 ] }
      ]);
      // now make sure the patches look good on merge
      let doc4 = doc.fork(undefined,blockHeads);
      let doc5 = create();
      doc5.put("/","a","b");
      doc5.updateDiffCursor();
      doc5.merge(doc4);
      let patches3D = doc5.diffIncremental();
      assert.deepStrictEqual(patches3D, [
        { action: 'put', path: [ 'text' ], value: '' },
        { action: 'splice', path: [ 'text', 0 ], value: 'aaabbbccc' },
        { action: 'insert', path: [ 'text', 3 ], values: [{}] },
        { action: 'put', path: [ 'text', 3, 'name' ], value: 'li' },
        { action: 'put', path: [ 'text', 3, 'parents' ], value: [] },
        { action: 'insert', path: [ 'text', 3, 'parents', 0 ], values: [ 'ul' ] }
      ]);
      let spans = doc5.spans("/text");
      assert.deepStrictEqual(spans, [
        { type: 'text', value: 'aaa' },
        { type: 'block', value: block },
        { type: 'text', value: 'bbbccc' }
      ])
    })
    it('references blocks on local changes', () => {
      let doc = create({ actor: "aabbcc" })
      let text = doc.putObject("_root", "text", "aaabbbccc")
      let block = { name: "li", parents: ["ul"] };
      let blockId = doc.splitBlock(text, 3, block);
      doc.updateDiffCursor();
      doc.splice("/text", 6, 0, "AAA");
      let patches = doc.diffIncremental();
      assert.deepStrictEqual(patches, [
        { action: 'splice', path: [ 'text', 6], value: 'AAA', block },
      ])
      let spans = doc.spans("/text");
      assert.deepStrictEqual(spans, [
        { type: 'text', value: 'aaa' },
        { type: 'block', value: block },
        { type: 'text', value: 'bbAAAbccc' }
      ])
      assert.deepStrictEqual(doc.objInfo("/"),
        { id: "_root", type: "map", path: [] }
      )
      assert.deepStrictEqual(doc.objInfo("/text"),
        { id: text, type: "text", path: ["text"] }
      )
      assert.deepStrictEqual(doc.objInfo("/text/3"),
        { id: blockId, type: "map", path: ["text", 3] }
      )
    })
  })
})
