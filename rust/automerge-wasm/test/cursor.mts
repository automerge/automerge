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

  describe('list cursors', () => {
    it('should be able to make a cursor from a position in a list, then use it', () => {
        let doc = create();
        let list = doc.putObject("/", "list", ["a", "b", "c", "d"]);
        let heads1 = doc.getHeads();

        // by object id and by path
        let cursor = doc.getCursor(list, 2);
        assert.deepStrictEqual(doc.getCursor("/list", 2), cursor);
        assert.deepStrictEqual(doc.getCursorPosition(list, cursor), 2);
        assert.deepStrictEqual(doc.getCursorPosition("/list", cursor), 2);

        // inserting before the element moves the cursor
        doc.insert(list, 0, "x");
        doc.splice(list, 1, 0, ["y", "z"]);
        assert.deepStrictEqual(doc.materialize("/list"), ["x", "y", "z", "a", "b", "c", "d"]);
        assert.deepStrictEqual(doc.getCursorPosition(list, cursor), 5);
        assert.deepStrictEqual(doc.get(list, 5), "c");

        // the cursor position at heads
        assert.deepStrictEqual(doc.getCursorPosition(list, cursor, heads1), 2);

        // a cursor at heads refers to the same element
        assert.deepStrictEqual(doc.getCursor(list, 2, heads1), cursor);
        assert.deepStrictEqual(doc.getCursor(list, 5), cursor);
    })

    it('should resolve cursors to deleted elements according to the move direction', () => {
        let doc = create();
        let list = doc.putObject("/", "list", ["a", "a", "a", "@", "b", "b", "b"]);
        let heads1 = doc.getHeads();
        let before = doc.getCursor(list, 3, undefined, "before");
        let after = doc.getCursor(list, 3, undefined, "after");
        let deflt = doc.getCursor(list, 3);

        doc.splice(list, 3, 1, ["~", "~", "~"]);
        assert.deepStrictEqual(doc.getCursorPosition(list, before), 2);
        assert.deepStrictEqual(doc.getCursorPosition(list, after), 6);
        assert.deepStrictEqual(doc.getCursorPosition(list, deflt), 6);
        assert.deepStrictEqual(doc.getCursorPosition(list, before, heads1), 3);

        // cursors created at old heads with a move direction
        let heads2 = doc.getHeads();
        doc.delete(list, 4);
        let beforeAt = doc.getCursor(list, 4, heads2, "before");
        let afterAt = doc.getCursor(list, 4, heads2, "after");
        assert.deepStrictEqual(doc.getCursorPosition(list, beforeAt), 3);
        assert.deepStrictEqual(doc.getCursorPosition(list, afterAt), 4);
    })

    it('should support start and end cursors on lists', () => {
        let doc = create();
        let list = doc.putObject("/", "list", [1, 2, 3]);
        let start = doc.getCursor(list, "start");
        let end = doc.getCursor(list, "end");
        let negative = doc.getCursor(list, -1);
        let pastEnd = doc.getCursor(list, 3);
        assert.deepStrictEqual(negative, start);
        assert.deepStrictEqual(pastEnd, end);

        doc.insert(list, 0, 0);
        doc.push(list, 4);
        assert.deepStrictEqual(doc.getCursorPosition(list, start), 0);
        assert.deepStrictEqual(doc.getCursorPosition(list, end), 5);
    })

    it('should support cursors to nested objects in a list', () => {
        let doc = create();
        let list = doc.putObject("/", "list", [{ a: 1 }, [1, 2]]);
        let cursor = doc.getCursor("/list", 1);
        let inner = doc.getCursor("/list/1", 1);
        doc.insertObject(list, 0, { b: 2 });
        doc.insert("/list/2", 0, 0);
        assert.deepStrictEqual(doc.getCursorPosition(list, cursor), 2);
        assert.deepStrictEqual(doc.getCursorPosition("/list/2", inner), 2);
    })

    it('should reject cursors on maps', () => {
        let doc = create();
        let list = doc.putObject("/", "list", [1]);
        let map = doc.putObject("/", "map", { a: 1 });
        let cursor = doc.getCursor(list, 0);
        assert.throws(() => doc.getCursor(map, 0), /cursors are only valid on lists and text - obj type: map/);
        assert.throws(() => doc.getCursor("/", 0), /cursors are only valid on lists and text - obj type: map/);
        assert.throws(() => doc.getCursorPosition(map, cursor), /cursors are only valid on lists and text - obj type: map/);
    })
  })
})
