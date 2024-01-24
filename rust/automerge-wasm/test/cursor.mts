import { describe, it } from 'mocha';
import assert from 'assert'
import { create } from '../nodejs/automerge_wasm.cjs'

describe('Automerge', () => {
  describe('text cursors', () => {
    it('it can make a cursor from a position in a text document, then use it', ()=> {
        const doc1 = create();
        doc1.putObject("/", "text", "the sly fox jumped over the lazy dog");
        const heads1 = doc1.getHeads();

        // get a cursor at a position
        const cursor = doc1.getCursor("/text", 12);
        const index1 = doc1.getCursorPosition("/text", cursor);
        assert.deepStrictEqual(index1, 12);

        // modifying the text changes the cursor position
        doc1.splice("/text",0,3,"Has the");
        assert.deepStrictEqual(doc1.text("/text"), "Has the sly fox jumped over the lazy dog");
        const index2 = doc1.getCursorPosition("/text", cursor);
        assert.deepStrictEqual(index2, 16);

        // get the cursor position at heads
        const index3 = doc1.getCursorPosition("/text", cursor, heads1);
        assert.deepStrictEqual(index1, index3);
 
        // get a cursor at heads
        const cursor2 = doc1.getCursor("/text", 12, heads1);
        const cursor3 = doc1.getCursor("/text", 16);
        assert.deepStrictEqual(cursor, cursor2);
        assert.deepStrictEqual(cursor, cursor3);

        // cursor works at the heads
        const cursor4 = doc1.getCursor("/text", 0);
        const index4 = doc1.getCursorPosition("/text", cursor4);
        assert.deepStrictEqual(index4, 0);
    })
    it('cursors can be used for textRange', ()=> {
        const doc1 = create();
        doc1.putObject("/", "text", "aXXXbbbXXXc");

        const heads1 = doc1.getHeads();

        let cursor1 = doc1.getCursor("/text",0)
        let cursor2 = doc1.getCursor("/text",10)

        doc1.splice("/text", 7, 3, "");
        doc1.splice("/text", 1, 3, "");

        assert.deepStrictEqual(doc1.text("/text"), "abbbc");
        assert.deepStrictEqual(doc1.textRange("/text","1..4"),"bbb");
        assert.deepStrictEqual(doc1.textRange("/text","1..4",heads1),"XXX");
        assert.deepStrictEqual(doc1.textRange("/text",`${cursor1}...${cursor2}`),"bbb");
        assert.deepStrictEqual(doc1.textRange("/text",`${cursor1}...${cursor2}`,heads1),"XXXbbbXXX");
        assert.deepStrictEqual(doc1.textRange("/text",`..${cursor2}`),"abbb");
        assert.deepStrictEqual(doc1.textRange("/text",`${cursor1}...`),"bbbc");
        assert.deepStrictEqual(doc1.textRange("/text",".."),"abbbc");
    })
    it('textRange can can handle inclusive and exclusive ranges', ()=> {
        const doc1 = create();
        doc1.putObject("/", "text", "0123456");

        assert.deepStrictEqual(doc1.textRange("/text", ".."), "0123456");
        assert.deepStrictEqual(doc1.textRange("/text", "..."), "0123456");
        assert.deepStrictEqual(doc1.textRange("/text", "..="), "0123456");

        assert.deepStrictEqual(doc1.textRange("/text", "2..5"), "234");
        assert.deepStrictEqual(doc1.textRange("/text", "..5"), "01234");
        assert.deepStrictEqual(doc1.textRange("/text", "2.."), "23456");

        assert.deepStrictEqual(doc1.textRange("/text", "2..=5"), "2345");
        assert.deepStrictEqual(doc1.textRange("/text", "..=5"), "012345");
        assert.deepStrictEqual(doc1.textRange("/text", "2..="), "23456");

        assert.deepStrictEqual(doc1.textRange("/text", "2...5"), "34");
        assert.deepStrictEqual(doc1.textRange("/text", "...5"), "01234");
        assert.deepStrictEqual(doc1.textRange("/text", "2..."), "3456");
    })
  })
})
