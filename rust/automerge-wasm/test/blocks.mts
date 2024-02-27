import { describe, it } from 'mocha';
//@ts-ignore
import assert from 'assert'
//@ts-ignore
import { create, load, Automerge, encodeChange, decodeChange } from '../nodejs/automerge_wasm.cjs'

describe('blocks', () => {
  it('can split a block', () => {
    let doc = create({ actor: "aabbcc" })
    let list = doc.putObject("_root", "list", "🐻🐻🐻bbbccc")
    doc.updateDiffCursor();
    let doc2 = doc.fork();
    doc.splitBlock(list, 6, { type: "li", parents: ["ul"] });
    const blockCursor = doc.getCursor("/list", 6);
    let patches1 = doc.diffIncremental();
    assert.deepStrictEqual(doc.text(list), "🐻🐻🐻\ufffcbbbccc")
    assert.deepStrictEqual(doc.length(list), 13)
    doc.joinBlock(list, 6);
    let patches2 = doc.diffIncremental();
    assert.deepStrictEqual(patches1, [
      {
        action: 'splitBlock',
        path: ['list', 6],
        index: 6,
        type: 'li',
        parents: ['ul'],
        cursor: blockCursor
      }
    ]);
    assert.deepStrictEqual(patches2, [
      { action: 'joinBlock', path: ['list',6], index:6 }
    ]);
    assert.deepStrictEqual(doc.text(list), "🐻🐻🐻bbbccc")
    assert.deepStrictEqual(doc.length(list), 12)

    doc2.updateDiffCursor();
    doc2.merge(doc);
    // TODO: Reintroduce this check that consecutive split and joins are elided
    //let patches3 = doc2.diffIncremental();
    //assert.deepStrictEqual(patches3, []);
  })
  it.skip('patches correctly reference blocks', () => {
    const doc = create({ actor: "aabbcc" })
    const text = doc.putObject("_root", "text", "aaabbbccc")
    const starterHeads = doc.getHeads();
    doc.updateDiffCursor();
    const doc2 = doc.fork();
    const block = { type: "li", parents: ["ul"] };
    doc.splitBlock(text, 3, block);
    const blockCursor = doc.getCursor("/text", 3);
    //doc.updateBlock(text, blockId, "div", ["block","pre"]);
    const blockHeads = doc.getHeads()
    doc.joinBlock(text, 3);
    doc.commit();
    //let patches = doc.diffIncremental();
    //assert.deepStrictEqual(patches,[]);
    assert.deepStrictEqual(doc.text(text), "aaabbbccc")
    assert.deepStrictEqual(doc.length(text), 9)

    doc2.updateDiffCursor();
    doc2.merge(doc);
    //let patches2 = doc2.diffIncremental();
    //assert.deepStrictEqual(patches2,[]); // insert and delete
    const doc3 = doc.fork(undefined,blockHeads);
    const patches3A = doc3.diff([],doc3.getHeads());
    const patches3B = doc.diff([],blockHeads);
    const patches3C = doc.diff(blockHeads, starterHeads);
    assert.deepStrictEqual(patches3A, [
      { action: 'put', path: [ 'text' ], value: '' },
      { action: 'splice', path: [ 'text', 0 ], value: 'aaa' },
      { action: 'insert', path: [ 'text', 3 ], values: [{}] },
      { action: 'splitBlock', path: [ 'text', 4 ], index: 4, value: 'bbbccc', block, cursor: blockCursor },
    ]);
    assert.deepStrictEqual(patches3A, patches3B);
    assert.deepStrictEqual(patches3C, [
      { action: "joinBlock", index: 3, path: [ "text", 3 ] }
    ]);
    // now make sure the patches look good on merge
    const doc4 = doc.fork(undefined,blockHeads);
    const doc5 = create();
    doc5.put("/","a","b");
    doc5.updateDiffCursor();
    doc5.merge(doc4);
    const patches3D = doc5.diffIncremental();
    assert.deepStrictEqual(patches3D, [
      { action: 'put', path: [ 'text' ], value: '' },
      { action: 'splice', path: [ 'text', 0 ], value: 'aaabbbccc' },
      { action: 'splitBlock', path: [ 'text', 3 ], index: 3, block, cursor: blockCursor },
    ]);
    const spans = doc5.spans("/text");
    assert.deepStrictEqual(spans, [
      { type: 'text', value: 'aaa' },
      { type: 'block', value: block },
      { type: 'text', value: 'bbbccc' }
    ])
  })
  it('references blocks on local changes', () => {
    let doc = create({ actor: "aabbcc" })
    let text = doc.putObject("_root", "text", "aaabbbccc")
    let block = { type: "li", parents: ["ul"] };
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
  })
  it("can update a block", () => {
    const doc = create({ actor: "aabbcc" })
    const text = doc.putObject("_root", "text", "aaabbbccc")
    const block = { type: "unordered-list-item", parents: [] };
    doc.splitBlock(text, 3, block);
    doc.updateDiffCursor()
    doc.updateBlock(text, 3, {type: "unordered-list-item", parents: ["unordered-list-item"]});
    const patches = doc.diffIncremental();
    assert.deepStrictEqual(patches, [{
      action: "updateBlock",
      path: ["text", 3],
    }]);
  })

  it.only("can update all blocks with a diff", () => {
    const doc = create()
    doc.putObject("_root","text", "");
    doc.splitBlock("/text", 0, {type: "ordered-list-item", parents: []})
    doc.splice("/text", 1, 0, "first thing");
    doc.splitBlock("/text", 12, {type: "ordered-list-item", parents: []})
    doc.splice("/text", 13, 0, "second thing");
    doc.updateBlocks("/text", [
      {type: "paragraph", parents: []},
      "the first thing",
      {type: "unordered-list-item", parents: ["ordered-list-item"]},
      "the second thing",
    ])
    const spansAfter = doc.spans("/text");
    assert.deepStrictEqual(spansAfter, [
      {type: "block", value: {type: "paragraph", parents: []}},
      {type: "text", value: "the first thing"},
      {type: "block", value: {type: "unordered-list-item", parents: ["ordered-list-item"]}},
      {type: "text", value: "the second thing"},
    ])
  })
})
