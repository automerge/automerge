import { describe, it } from 'mocha';
//@ts-ignore
import assert from 'assert'
//@ts-ignore
import { create, load, Automerge, encodeChange, decodeChange, ObjID } from '../nodejs/automerge_wasm.cjs'

describe('blocks', () => {
  describe("when splitting a block", () => {

    it("can split a block", () => {
      const doc = create({ actor: "aabbcc" })
      const text = doc.putObject("_root", "list", "🐻🐻🐻bbbccc")
      doc.splitBlock(text, 6, { type: "li", parents: ["ul"], attrs: {kind: "todo" }});
      const spans = doc.spans("/list");
      assert.deepStrictEqual(spans, [
        { type: "text", value: "🐻🐻🐻" },
        { type: 'block', value: { type: 'li', parents: ['ul'], attrs: {kind: "todo"} } },
        { type: 'text', value: 'bbbccc' }
      ])
    })

    it("produces local incremental patches", () => {
      const doc = create({ actor: "aabbcc" })
      const text = doc.putObject("_root", "text", "🐻🐻🐻bbbccc")
      doc.updateDiffCursor()
      doc.splitBlock(text, 6, { type: "li", parents: ["ul"], attrs: {kind: "todo"} });
      const patches = doc.diffIncremental()
      assert.deepStrictEqual(patches, [{
          action: 'splitBlock',
          path: ['text', 6],
          index: 6,
          type: 'li',
          parents: ['ul'],
          attrs: {kind: "todo"},
          cursor: doc.getCursor("/text", 6)
      }])
    })

    it("produces remote incremental patches", () => {
      const doc = create({ actor: "aabbcc" })
      const text = doc.putObject("_root", "text", "🐻🐻🐻bbbccc")
      const doc2 = doc.fork()
      doc2.updateDiffCursor()
      doc.splitBlock(text, 6, { type: "li", parents: ["ul"], attrs: {kind: "todo"} });
      doc2.merge(doc)
      const patches = doc2.diffIncremental()
      assert.deepStrictEqual(patches, [{
          action: 'splitBlock',
          path: ['text', 6],
          index: 6,
          type: 'li',
          parents: ['ul'],
          attrs: {kind: "todo"},
          cursor: doc.getCursor("/text", 6)
      }])

    })

    it("produces full scan patches", () => {
      const doc = create({ actor: "aabbcc" })
      const text = doc.putObject("_root", "text", "🐻🐻🐻bbbccc")
      const headsBefore = doc.getHeads()
      doc.splitBlock(text, 6, { type: "li", parents: ["ul"], attrs: { kind: "todo"} });
      const headsAfter = doc.getHeads()
      doc.resetDiffCursor()
      const patches = doc.diff(headsBefore, headsAfter)
      assert.deepStrictEqual(patches, [{
          action: 'splitBlock',
          path: ['text', 6],
          index: 6,
          type: 'li',
          parents: ['ul'],
          attrs: {kind: "todo"},
          cursor: doc.getCursor("/text", 6)
      }])
    })

    it.skip('consolidates patches', () => {
      const doc = create({ actor: "aabbcc" })
      const list = doc.putObject("_root", "list", "🐻🐻🐻bbbccc")
      doc.updateDiffCursor();
      const doc2 = doc.fork();
      doc.splitBlock(list, 6, { type: "li", parents: ["ul"], attrs: {} });
      doc.joinBlock(list, 6);
      assert.deepStrictEqual(doc.text(list), "🐻🐻🐻bbbccc")
      assert.deepStrictEqual(doc.length(list), 12)

      doc2.updateDiffCursor();
      doc2.merge(doc);
      const patches3 = doc2.diffIncremental();
      assert.deepStrictEqual(patches3, []);
    })
  })

  describe("when joining a block", () => {
    let doc: Automerge
    let text: ObjID
    const block = { type: "unordered-list-item", parents: [], attrs: {} };

    beforeEach(() => {
      doc = create({ actor: "aabbcc" })
      text = doc.putObject("_root", "text", "aaabbbccc")
      doc.splitBlock(text, 3, block);
      doc.updateDiffCursor()
    })

    it("can join a block", () => {
      doc.joinBlock(text, 3);
      const spans = doc.spans("/text");
      assert.deepStrictEqual(spans, [
        { type: 'text', value: 'aaabbbccc' }
      ])
    })

    it("produces local incremental patches", () => {
      doc.joinBlock(text, 3);
      const patches = doc.diffIncremental()
      assert.deepStrictEqual(patches, [{
        action: "joinBlock",
        path: ["text", 3],
        index: 3
      }])
    })

    it("produces remote incremental patches", () => {
      const doc2 = doc.fork()
      doc2.updateDiffCursor()
      doc.joinBlock(text, 3);
      doc2.merge(doc)
      const patches = doc2.diffIncremental()
      assert.deepStrictEqual(patches, [{
        action: "joinBlock",
        path: ["text", 3],
        index: 3
      }])
    })

    it("produces full scan patches", () => {
      const headsBefore = doc.getHeads()
      doc.joinBlock(text, 3);
      const headsAfter = doc.getHeads()
      doc.resetDiffCursor()
      const patches = doc.diff(headsBefore, headsAfter)
      assert.deepStrictEqual(patches, [{
        action: "joinBlock",
        path: ["text", 3],
        index: 3
      }])
    })
  })

  it.skip('patches correctly reference blocks', () => {
    const doc = create({ actor: "aabbcc" })
    const text = doc.putObject("_root", "text", "aaabbbccc")
    const starterHeads = doc.getHeads();
    doc.updateDiffCursor();
    const doc2 = doc.fork();
    const block = { type: "li", parents: ["ul"], attrs: {} };
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
    const doc = create({ actor: "aabbcc" })
    const text = doc.putObject("_root", "text", "aaabbbccc")
    const block = { type: "li", parents: ["ul"], attrs: {} };
    doc.splitBlock(text, 3, block);
    doc.updateDiffCursor();
    doc.splice("/text", 6, 0, "AAA");
    const patches = doc.diffIncremental();
    assert.deepStrictEqual(patches, [
      { action: 'splice', path: [ 'text', 6], value: 'AAA', block },
    ])
    const spans = doc.spans("/text");
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

  describe("when updating a block", () => {
    let doc: Automerge
    let text: ObjID
    const block = { type: "unordered-list-item", parents: [], attrs: {} };

    beforeEach(() => {
      doc = create({ actor: "aabbcc" })
      text = doc.putObject("_root", "text", "aaabbbccc")
      doc.splitBlock(text, 3, block);
      doc.updateDiffCursor()
    })

    describe("when updating a block type", () => {
      it("can update a block type", () => {
        doc.updateBlock(text, 3, {type: "ordered-list-item", parents: [], attrs: {} });
        const spans = doc.spans("/text");
        assert.deepStrictEqual(spans, [
          { type: 'text', value: 'aaa' },
          { type: 'block', value: { type: 'ordered-list-item', parents: [], attrs: {} } },
          { type: 'text', value: 'bbbccc' }
        ])
      })

      it("produces local incremental patches", () => {
        doc.updateBlock(text, 3, {type: "ordered-list-item", parents: [], attrs: {} });
        const patches = doc.diffIncremental();
        assert.deepStrictEqual(patches, [{
          action: "updateBlock",
          path: ["text", 3],
          index: 3,
          new_type: "ordered-list-item",  
          new_parents: null,
          new_attrs: null,
        }]);
      })

      it("produces remote incremental patches", () => {
        const doc2 = doc.fork()
        doc2.updateDiffCursor()
        doc.updateBlock(text, 3, {type: "ordered-list-item", parents: [], attrs: {} });
        doc2.merge(doc)
        const patches = doc2.diffIncremental()
        assert.deepStrictEqual(patches, [{
          action: "updateBlock",
          path: ["text", 3],
          index: 3,
          new_type: "ordered-list-item",  
          new_parents: null,
          new_attrs: null,
        }])
      })

      it("produces full scan patches", () => {
        const headsBefore = doc.getHeads()
        doc.updateBlock(text, 3, {type: "ordered-list-item", parents: [], attrs: {} });
        const headsAfter = doc.getHeads()
        doc.resetDiffCursor()
        const patches = doc.diff(headsBefore, headsAfter)
        assert.deepStrictEqual(patches, [{
          action: "updateBlock",
          path: ["text", 3],
          index: 3,
          new_type: "ordered-list-item",  
          new_parents: null,
          new_attrs: null,
        }])
      })
    })

    describe("when updating block parents", () => {
      it("can update the block parents", () => {
        doc.updateBlock(text, 3, {type: "unordered-list-item", parents: ["ordered-list-item"], attrs: {} });
        const spans = doc.spans("/text")
        assert.deepStrictEqual(spans, [
          { type: 'text', value: 'aaa' },
          { type: 'block', value: { type: 'unordered-list-item', parents: ['ordered-list-item'], attrs: {} } },
          { type: 'text', value: 'bbbccc' }
        ])
      })

      it("produces local incremental patches", () => {
        doc.updateBlock(text, 3, {type: "unordered-list-item", parents: ["ordered-list-item"], attrs: {} });
        const patches = doc.diffIncremental();
        assert.deepStrictEqual(patches, [{
          action: "updateBlock",
          path: ["text", 3],
          index: 3,
          new_type: null,
          new_attrs: null,
          new_parents: ["ordered-list-item"],  
        }]);
      })

      it("produces remote incremental patches", () => {
        const doc2 = doc.fork()
        doc2.updateDiffCursor()
        doc.updateBlock(text, 3, {type: "unordered-list-item", parents: ["ordered-list-item"], attrs: {} });
        doc2.merge(doc)
        const patches = doc2.diffIncremental()
        assert.deepStrictEqual(patches, [{
          action: "updateBlock",
          path: ["text", 3],
          index: 3,
          new_type: null,
          new_attrs: null,
          new_parents: ["ordered-list-item"],  
        }])
      })

      it("produces full scan patches", () => {
        const headsBefore = doc.getHeads()
        doc.updateBlock(text, 3, {type: "unordered-list-item", parents: ["ordered-list-item"], attrs: {} });
        const headsAfter = doc.getHeads()
        doc.updateDiffCursor()
        const patches = doc.diff(headsBefore, headsAfter)
        assert.deepStrictEqual(patches, [{
          action: "updateBlock",
          path: ["text", 3],
          index: 3,
          new_type: null,
          new_attrs: null,
          new_parents: ["ordered-list-item"],  
        }])
      })
    })

    describe("when updating block attributes", () => {
      it("can update block attributes", () => {
        doc.updateBlock(text, 3, {type: "unordered-list-item", parents: ["ordered-list-item"], attrs: {} });
        const spans = doc.spans("/text")
        assert.deepStrictEqual(spans, [
          { type: 'text', value: 'aaa' },
          { type: 'block', value: { type: 'unordered-list-item', parents: ['ordered-list-item'], attrs: {} } },
          { type: 'text', value: 'bbbccc' }
        ])
      })
  
      it("produces local incremental patches", () => {
        doc.updateBlock(text, 3, {type: "unordered-list-item", parents: [], attrs: {foo: "bar"} });
        const patches = doc.diffIncremental();
        assert.deepStrictEqual(patches, [{
          action: "updateBlock",
          path: ["text", 3],
          index: 3,
          new_type: null,
          new_parents: null,
          new_attrs: {foo: "bar"}
        }]);
      })

      it("produces remote incremental patches", () => {
        const doc2 = doc.fork()
        doc2.updateDiffCursor()
        doc.updateBlock(text, 3, {type: "unordered-list-item", parents: [], attrs: {foo: "bar"} });
        doc2.merge(doc)
        const patches = doc2.diffIncremental()
        assert.deepStrictEqual(patches, [{
          action: "updateBlock",
          path: ["text", 3],
          index: 3,
          new_type: null,
          new_parents: null,
          new_attrs: {foo: "bar"}
        }])
      })

      it("produces full scan patches", () => {
        const headsBefore = doc.getHeads()
        doc.updateBlock(text, 3, {type: "unordered-list-item", parents: [], attrs: {foo: "bar"} });
        const headsAfter = doc.getHeads()
        doc.resetDiffCursor()
        const patches = doc.diff(headsBefore, headsAfter)
        assert.deepStrictEqual(patches, [{
          action: "updateBlock",
          path: ["text", 3],
          index: 3,
          new_type: null,
          new_parents: null,
          new_attrs: {foo: "bar"}
        }])
      })
    })
  })

  describe("when updating all blocks via a diff", () => {
    it("can update multiple spans", () => {
      const doc = create()
      doc.putObject("_root","text", "");
      doc.splitBlock("/text", 0, {type: "ordered-list-item", parents: [], attrs: {kind: "todo"}})
      doc.splice("/text", 1, 0, "first thing");
      doc.splitBlock("/text", 12, {type: "ordered-list-item", parents: [], attrs: {kind: "todo"}});
      doc.splice("/text", 13, 0, "second thing");
      doc.updateBlocks("/text", [
        {type: "paragraph", parents: [], attrs: {kind: "reallytodo"}},
        "the first thing",
        {type: "unordered-list-item", parents: ["ordered-list-item"], attrs: {}},
        "the second thing",
      ])
      const spansAfter = doc.spans("/text");
      console.log(JSON.stringify(spansAfter, null, 2))
      assert.deepStrictEqual(spansAfter, [
        {type: "block", value: {type: "paragraph", parents: [], attrs: {kind: "reallytodo"}}},
        {type: "text", value: "the first thing"},
        {type: "block", value: {type: "unordered-list-item", parents: ["ordered-list-item"], attrs: {}}},
        {type: "text", value: "the second thing"},
      ])
    })
  })

  describe("when registering a datatype", () => {
    it("should call the register datatype function with a context argument", () => {
      const doc = create()

      class AttrString {
        constructor(public value: string) {}
      }

      doc.registerDatatype("str", (value: string, {context}: {context: string}) => {
        if (context === "blockAttr") {
          return new AttrString(value)
        } else {
          return value
        }
      })

      doc.putObject("_root", "text", "aaabbbccc")
      doc.splitBlock("/text", 0, {type: "paragraph", parents: [], attrs: {kind: "todo"}});

      const block = doc.getBlock("/text", 0)
      if (block == null) throw new Error("block is null")
      assert.deepStrictEqual(block.attrs.kind, new AttrString("todo"))
    })
  })
})
