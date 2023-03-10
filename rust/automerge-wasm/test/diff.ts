import { describe, it } from 'mocha';
//@ts-ignore
import assert from 'assert'
//@ts-ignore
import { create } from '..'


let util = require('util')

describe('Automerge', () => {
  describe('diff', () => {
    it('it should be able to handle a simple incremental diff', ()=> {
        let doc1 = create(true);
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
        let doc1 = create(true);
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
        let doc1 = create(true);
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
  })
})
