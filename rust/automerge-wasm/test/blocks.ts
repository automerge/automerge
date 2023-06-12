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
      let list = doc.putObject("_root", "list", "aaabbbccc")
      doc.updateDiffCursor();
      let doc2 = doc.fork();
      let blockId = doc.splitBlock(list, 3, "li", ["ul"]);
      doc.updateBlock(list, blockId, "div", ["block","pre"]);
      doc.joinBlock(list, blockId);
      doc.commit();
      let patches = doc.diffIncremental();
      assert.deepStrictEqual(patches, [
        { action: 'splitBlock', path: ['list', 3],
          name: 'div', parents: ['block', "pre"], cursor: blockId,
        },
        { action: 'joinBlock', path: ['list', 3],
          cursor: blockId,
        },
      ]);
      assert.deepStrictEqual(doc.text(list), "aaabbbccc")
      assert.deepStrictEqual(doc.length(list), 9)

      doc2.updateDiffCursor();
      doc2.merge(doc);
      let patches2 = doc2.diffIncremental();
      assert.deepStrictEqual(patches2, [
        { action: 'splitBlock', path: ['list', 3],
          name: 'div', parents: ['block', "pre"], cursor: blockId,
        },
        { action: 'joinBlock', path: ['list', 3],
          cursor: blockId,
        },
      ]);
    })
    it('patches correctly reference blocks', () => {
      let doc = create({ actor: "aabbcc" })
      let list = doc.putObject("_root", "list", "aaabbbccc")
      let starterHeads = doc.getHeads();
      doc.updateDiffCursor();
      let doc2 = doc.fork();
      let blockId = doc.splitBlock(list, 3, "li", ["ul"]);
      doc.updateBlock(list, blockId, "div", ["block","pre"]);
      let blockHeads = doc.getHeads()
      doc.joinBlock(list, blockId);
      doc.commit();
      let patches = doc.diffIncremental();
      assert.deepStrictEqual(patches, [
        { action: 'splitBlock', path: ['list', 3],
          name: 'div', parents: ['block', "pre"], cursor: blockId,
        },
        { action: 'joinBlock', path: ['list', 3],
          cursor: blockId,
        },
      ]);
      assert.deepStrictEqual(doc.text(list), "aaabbbccc")
      assert.deepStrictEqual(doc.length(list), 9)

      doc2.updateDiffCursor();
      doc2.merge(doc);
      let patches2 = doc2.diffIncremental();
      assert.deepStrictEqual(patches2, [
        { action: 'splitBlock', path: ['list', 3],
          name: 'div', parents: ['block', "pre"], cursor: blockId,
        },
        { action: 'joinBlock', path: ['list', 3],
          cursor: blockId,
        },
      ]);
      let doc3 = doc.fork(undefined,blockHeads);
      let patches3A = doc3.diff([],doc3.getHeads());
      let patches3B = doc.diff([],blockHeads);
      let patches3C = doc.diff(blockHeads, starterHeads);
      assert.deepStrictEqual(patches3A, [
        { action: 'put', path: [ 'list' ], value: '' },
        { action: 'splice', 
          path: [ 'list', 0 ],
          value: 'aaa',
        },
        { action: 'splice', 
          path: [ 'list', 3 ],
          value: 'bbbccc',
          block: { name: 'div', parents: [ 'block', 'pre' ] },
          marks: {},
        }
      ]);
      assert.deepStrictEqual(patches3A, patches3B);
      assert.deepStrictEqual(patches3C, [
        { action: "joinBlock", cursor: blockId, path: [ "list", 3 ] }
      ]);
      // now make sure the patches look good on merge
      let doc4 = doc.fork(undefined,blockHeads);
      let doc5 = create();
      doc5.put("/","a","b");
      doc5.updateDiffCursor();
      doc5.merge(doc4);
      let patches3D = doc5.diffIncremental();
      assert.deepStrictEqual(patches3D, [
        { action: 'put', path: [ 'list' ], value: '' },
        { action: 'splice', path: [ 'list', 0 ], value: 'aaabbbccc' },
        {
          action: 'splitBlock',
          path: [ 'list', 3 ],
          name: 'div',
          parents: [ 'block', 'pre' ],
          cursor: '11@aabbcc'
        }
      ]);
    })
  })
})
