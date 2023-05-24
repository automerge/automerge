import { describe, it } from 'mocha';
//@ts-ignore
import assert from 'assert'
//@ts-ignore
import { create } from '..'


let util = require('util')

describe('Automerge', () => {
  describe('diff', () => {
    it('it should be able to handle a simple incremental diff', ()=> {
        let doc1 = create();
        doc1.put("/", "key1", "value1");
        let heads1 = doc1.getHeads();
        doc1.put("/", "key1", "value2");
        let heads2 = doc1.getHeads();
        doc1.put("/", "key1", "value3");
        let patches12 = doc1.diff(heads1,heads2);
        let patches21 = doc1.diff(heads2,heads1);
        let patches11 = doc1.diff(heads1,heads1);
        assert.deepStrictEqual(patches12, [
          { action: "put", path: ["key1"], value: "value2" }
        ])
        assert.deepStrictEqual(patches21, [
          { action: "put", path: ["key1"], value: "value1" }
        ])
        assert.deepStrictEqual(patches11, [])
    })

    it('it should be able to handle diffs in sub objects', ()=> {
        let doc1 = create();
        doc1.putObject("/", "list", [0,1,2,3,4,5,6]);
        doc1.putObject("/list", 3,  { hello: "world" });
        let heads1 = doc1.getHeads();
        doc1.put("/list/3", "hello", "everyone");
        doc1.delete("/list", 2);
        let heads2 = doc1.getHeads();
        let patches12 = doc1.diff(heads1,heads2);
        let patches21 = doc1.diff(heads2,heads1);
        let patches11 = doc1.diff(heads1,heads1);
        assert.deepStrictEqual(patches12, [
          { action: "del", path: ["list", 2 ]  },
          { action: "put", path: ["list", 2, "hello"], value: "everyone" }
        ])
        assert.deepStrictEqual(patches21, [
          { action: "insert", path: ["list", 2 ], values: [2]  },
          { action: "put", path: ["list", 3, "hello"], value: "world" }
        ])
        assert.deepStrictEqual(patches11, [])
    })
    it('it should be able to handle text splices', ()=> {
        let doc1 = create();
        doc1.putObject("/", "text", "the quick fox jumps over the lazy dog");
        let heads1 = doc1.getHeads();
        doc1.splice("/text", 10, 3, "cow");
        let heads2 = doc1.getHeads();
        let patches12 = doc1.diff(heads1,heads2);
        let patches21 = doc1.diff(heads2,heads1);
        let patches11 = doc1.diff(heads1,heads1);
        assert.deepStrictEqual(patches12, [
          { action: "splice", path: ["text", 10], value: "cow" },
          { action: "del", path: ["text", 13], length: 3 },
        ])
        assert.deepStrictEqual(patches21, [
          { action: "del", path: ["text", 10], length: 3 },
          { action: "splice", path: ["text", 10], value: "fox" },
        ])
        assert.deepStrictEqual(patches11, [])
    })
    it('it should be able to handle diffing simple marks', () => {
        let doc1 = create();
        let text = doc1.putObject("/", "text", "the quick fox jumps over the lazy dog");
        let heads1 = doc1.getHeads();
        doc1.mark(text, { start: 3, end: 6 } , "bold" , true)
        let heads2 = doc1.getHeads();
        let patches12 = doc1.diff(heads1,heads2);
        let patches21 = doc1.diff(heads2,heads1);
        assert.deepStrictEqual(patches12, [
          { action: "mark", path: ["text"], marks: [ { start: 3, end: 6, name: "bold", value: true } ] },
        ])
        assert.deepStrictEqual(patches21, [
          { action: "mark", path: ["text"], marks: [ { start: 3, end: 6, name: "bold", value: null } ] },
        ])
    })
    it('it should be able to handle diffing complex marks', () => {
        let doc1 = create();
        let text = doc1.putObject("/", "text", "the quick fox jumps over the lazy dog");
        doc1.mark(text, { start: 0, end: 37 } , "bold" , true)
        doc1.mark(text, { start: 5, end: 10 } , "font" , 'san-serif')
        doc1.mark(text, { start: 20, end: 25 } , "font" , 'san-serif')
        let heads1 = doc1.getHeads();
        doc1.mark(text, { start: 0, end: 37 } , "font" , 'monospace')
        doc1.mark(text, { start: 5, end: 10 } , "bold" , false)
        doc1.mark(text, { start: 20, end: 25 } , "bold" , false)
        let heads2 = doc1.getHeads();
        let patches12 = doc1.diff(heads1,heads2);
        let patches21 = doc1.diff(heads2,heads1);
        assert.deepStrictEqual(patches12, [
          { action: "mark", path: ["text"], marks: [
            { start: 5, end: 10, name: "bold", value: false },
            { start: 20, end: 25, name: "bold", value: false },
            { start: 0, end: 37, name: "font", value: "monospace" },
          ] },
        ])
        assert.deepStrictEqual(patches21, [
          { action: "mark", path: ["text"], marks: [
            { start: 5, end: 10, name: "bold", value: true },
            { start: 20, end: 25, name: "bold", value: true },
            { start: 0, end: 5, name: "font", value: null },
            { start: 5, end: 10, name: "font", value: "san-serif" },
            { start: 10, end: 20, name: "font", value: null },
            { start: 20, end: 25, name: "font", value: "san-serif" },
            { start: 25, end: 37, name: "font", value: null },
          ] },
        ])
    })
    it('it should be able to handle diffing complex marks (2)', () => {
        let doc1 = create();
        let text = doc1.putObject("/", "text", "the quick fox jumps over the lazy dog");
        doc1.mark(text, { start: 0, end: 10 } , "bold" , true)
        doc1.mark(text, { start: 15, end: 17 } , "bold" , true)
        doc1.mark(text, { start: 25, end: 35 } , "bold" , true)
        let heads1 = doc1.getHeads();
        doc1.mark(text, { start: 8, end: 15 } , "bold" , false)
        doc1.mark(text, { start: 20, end: 27 } , "bold" , false)
        let heads2 = doc1.getHeads();
        let patches12 = doc1.diff(heads1,heads2);
        let patches21 = doc1.diff(heads2,heads1);
        assert.deepStrictEqual(patches12, [
          { action: "mark", path: ["text"], marks: [
            { start: 8, end: 15, name: "bold", value: false },
            { start: 20, end: 27, name: "bold", value: false },
          ] },
        ])
        assert.deepStrictEqual(patches21, [
          { action: "mark", path: ["text"], marks: [
            { start: 8, end: 10, name: "bold", value: true },
            { start: 20, end: 25, name: "bold", value: null },
            { start: 25, end: 27, name: "bold", value: true },
            { start: 10, end: 15, name: "bold", value: null },
          ] },
        ])
    })

    it('it should be able to handle exposing objects in maps', () => {
      let doc1 = create({ actor: "aaaa" })
      let map = doc1.putObject("/", "map", { foo: "bar" });
      let doc2 = doc1.fork("bbbb")
      doc1.truncatePatches()
      doc1.putObject("/map", "foo", { from: "doc1", other: 1 })
      let patches1 = doc1.popPatches();
      let heads1 = doc1.getHeads()
      assert.deepStrictEqual(patches1, [
        { action: 'put', path: [ 'map', 'foo' ], value: {} },
        { action: 'put', path: [ 'map', 'foo', 'from' ], value: 'doc1' },
        { action: 'put', path: [ 'map', 'foo', 'other' ], value: 1 }
      ])
      doc2.putObject("/map", "foo", { from: "doc2", something: 2 })
      doc1.put("/map/foo", "other", 10);
      doc1.merge(doc2)
      let patches2 = doc1.popPatches();
      let heads2 = doc1.getHeads()
      assert.deepStrictEqual(patches2, [
        { action: 'put', path: [ 'map', 'foo' ], value: {} },
        { action: 'put', path: [ 'map', 'foo', 'from' ], value: 'doc2' },
        { action: 'put', path: [ 'map', 'foo', 'something' ], value: 2 }
      ])
      doc2.delete("/map", "foo")
      doc1.merge(doc2)
      let patches3 = doc1.popPatches();
      let heads3 = doc1.getHeads()
      assert.deepStrictEqual(patches3, [
        { action: 'put', path: [ 'map', 'foo' ], value: {} },
        { action: 'put', path: [ 'map', 'foo', 'from' ], value: 'doc1' },
        { action: 'put', path: [ 'map', 'foo', 'other' ], value: 10 }
      ])
      assert.deepStrictEqual(doc1.diff(heads3, heads2), patches2)
      assert.deepStrictEqual(doc1.diff(heads2, heads3), patches3)
    })

    it('it should be able to handle exposing objects in lists', () => {
      let doc1 = create({ actor: "aaaa" })
      let list = doc1.putObject("/", "list", [ 0 ,1, 2 ]);
      let doc2 = doc1.fork("bbbb")
      doc1.truncatePatches()
      let heads1 = doc1.getHeads()
      doc1.putObject("/list", 1, { from: "doc1", other: 1 })
      let patches1 = doc1.popPatches();
      assert.deepStrictEqual(patches1, [
        { action: 'put', path: [ 'list', 1 ], value: {} },
        { action: 'put', path: [ 'list', 1, 'from' ], value: 'doc1' },
        { action: 'put', path: [ 'list', 1, 'other' ], value: 1 }
      ])
      doc2.putObject("/list", 1, { from: "doc2", something: 2 })
      doc1.put("/list/1", "other", 10);
      doc1.merge(doc2)
      let patches2 = doc1.popPatches();
      let heads2 = doc1.getHeads()
      assert.deepStrictEqual(patches2, [
        { action: 'put', path: [ 'list', 1 ], value: {} },
        { action: 'put', path: [ 'list', 1, 'from' ], value: 'doc2' },
        { action: 'put', path: [ 'list', 1, 'something' ], value: 2 }
      ])
      doc2.delete("/list", 1)
      doc1.merge(doc2)
      let patches3 = doc1.popPatches();
      let heads3 = doc1.getHeads()
      assert.deepStrictEqual(patches3, [
        { action: 'put', path: [ 'list', 1 ], value: {} },
        { action: 'put', path: [ 'list', 1, 'from' ], value: 'doc1' },
        { action: 'put', path: [ 'list', 1, 'other' ], value: 10 }
      ])
      assert.deepStrictEqual(doc1.diff(heads3, heads2), patches2)
      assert.deepStrictEqual(doc1.diff(heads2, heads3), patches3)
    })
  })
})
