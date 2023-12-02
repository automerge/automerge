import { describe, it } from 'mocha';
//@ts-ignore
import assert from 'assert'
//@ts-ignore
import { create } from '../nodejs/automerge_wasm.cjs'

describe('Automerge', () => {
  describe('text cursors', () => {
    it('it should be able to make a cursor from a position in a text document, then use it', ()=> {
        let doc1 = create();
        doc1.putObject("/", "text", "the sly fox jumped over the lazy dog");
        let heads1 = doc1.getHeads();

        // get a cursor at a position
        let cursor = doc1.getCursor("/text", 12);
        let index1 = doc1.getCursorPosition("/text", cursor);
        assert.deepStrictEqual(index1, 12);

        // modifying the text changes the cursor position
        doc1.splice("/text",0,3,"Has the");
        assert.deepStrictEqual(doc1.text("/text"), "Has the sly fox jumped over the lazy dog");
        let index2 = doc1.getCursorPosition("/text", cursor);
        assert.deepStrictEqual(index2, 16);

        // get the cursor position at heads
        let index3 = doc1.getCursorPosition("/text", cursor, heads1);
        assert.deepStrictEqual(index1, index3);
 
        // get a cursor at heads
        let cursor2 = doc1.getCursor("/text", 12, heads1);
        let cursor3 = doc1.getCursor("/text", 16);
        assert.deepStrictEqual(cursor, cursor2);
        assert.deepStrictEqual(cursor, cursor3);

        // cursor works at the heads
        let cursor4 = doc1.getCursor("/text", 0);
        let index4 = doc1.getCursorPosition("/text", cursor4);
        assert.deepStrictEqual(index4, 0);
    })
  })
})
