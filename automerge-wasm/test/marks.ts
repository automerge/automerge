import { describe, it } from 'mocha';
//@ts-ignore
import assert from 'assert'
//@ts-ignore
import { create, loadDoc, Automerge, TEXT, encodeChange, decodeChange } from '../dev/index'

describe('Automerge', () => {
  describe('marks', () => {
    it('should handle marks [..]', () => {
      let doc = create()
       let list = doc.set("_root", "list", TEXT)
      if (!list) throw new Error('should not be undefined')
       doc.splice(list, 0, 0, "aaabbbccc")
       doc.mark(list, "[3..6]", "bold" , true)
      let spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaa', [ [ 'bold', 'boolean', true ] ], 'bbb', [], 'ccc' ]);
      doc.insert(list, 6, "A")
      doc.insert(list, 3, "A")
      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaaA', [ [ 'bold', 'boolean', true ] ], 'bbb', [], 'Accc' ]);
    })

    it('should handle marks with deleted ends [..]', () => {
      let doc = create()
      let list = doc.set("_root", "list", TEXT)
      if (!list) throw new Error('should not be undefined')

      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "[3..6]", "bold" , true)
      let spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaa', [ [ 'bold', 'boolean', true ] ], 'bbb', [], 'ccc' ]);
      doc.del(list,5);
      doc.del(list,5);
      doc.del(list,2);
      doc.del(list,2);
      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aa', [ [ 'bold', 'boolean', true ] ], 'b', [], 'cc' ])
      doc.insert(list, 3, "A")
      doc.insert(list, 2, "A")
      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaA', [ [ 'bold', 'boolean', true ] ], 'b', [], 'Acc' ])
    })

    it('should handle sticky marks (..)', () => {
      let doc = create()
      let list = doc.set("_root", "list", TEXT)
      if (!list) throw new Error('should not be undefined')
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "(3..6)", "bold" , true)
      let spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaa', [ [ 'bold', 'boolean', true ] ], 'bbb', [], 'ccc' ]);
      doc.insert(list, 6, "A")
      doc.insert(list, 3, "A")
      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaa', [ [ 'bold', 'boolean', true ] ], 'AbbbA', [], 'ccc' ]);
    })

    it('should handle sticky marks with deleted ends (..)', () => {
      let doc = create()
      let list = doc.set("_root", "list", TEXT)
      if (!list) throw new Error('should not be undefined')
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "(3..6)", "bold" , true)
      let spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaa', [ [ 'bold', 'boolean', true ] ], 'bbb', [], 'ccc' ]);
      doc.del(list,5);
      doc.del(list,5);
      doc.del(list,2);
      doc.del(list,2);
      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aa', [ [ 'bold', 'boolean', true ] ], 'b', [], 'cc' ])
      doc.insert(list, 3, "A")
      doc.insert(list, 2, "A")
      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aa', [ [ 'bold', 'boolean', true ] ], 'AbA', [], 'cc' ])

      // make sure save/load can handle marks

      let doc2 = loadDoc(doc.save())
      spans = doc2.spans(list);
      assert.deepStrictEqual(spans, [ 'aa', [ [ 'bold', 'boolean', true ] ], 'AbA', [], 'cc' ])

      assert.deepStrictEqual(doc.getHeads(), doc2.getHeads())
      assert.deepStrictEqual(doc.save(), doc2.save())
    })

    it('should handle overlapping marks', () => {
      let doc : Automerge = create("aabbcc")
      let list = doc.set("_root", "list", TEXT)
      if (!list) throw new Error('should not be undefined')
      doc.splice(list, 0, 0, "the quick fox jumps over the lazy dog")
      doc.mark(list, "[0..37]", "bold" , true)
      doc.mark(list, "[4..19]", "itallic" , true)
      doc.mark(list, "[10..13]", "comment" , "foxes are my favorite animal!")
      doc.commit("marks",999);
      let spans = doc.spans(list);
      assert.deepStrictEqual(spans,
        [
          [ [ 'bold', 'boolean', true ] ],
          'the ',
          [ [ 'bold', 'boolean', true ], [ 'itallic', 'boolean', true ] ],
          'quick ',
          [
            [ 'bold', 'boolean', true ],
            [ 'comment', 'str', 'foxes are my favorite animal!' ],
            [ 'itallic', 'boolean', true ]
          ],
          'fox',
          [ [ 'bold', 'boolean', true ], [ 'itallic', 'boolean', true ] ],
          ' jumps',
          [ [ 'bold', 'boolean', true ] ],
          ' over the lazy dog',
          [],
        ]
      )
      let text = doc.text(list);
      assert.deepStrictEqual(text, "the quick fox jumps over the lazy dog");
      let raw_spans = doc.raw_spans(list);
      assert.deepStrictEqual(raw_spans,
        [
          { id: "39@aabbcc", time: 999, start: 0, end: 37, type: 'bold', value: true },
          { id: "41@aabbcc", time: 999, start: 4, end: 19, type: 'itallic', value: true },
          { id: "43@aabbcc", time: 999, start: 10, end: 13, type: 'comment', value: 'foxes are my favorite animal!' }
        ]);

      // mark sure encode decode can handle marks

      let all = doc.getChanges([])
      let decoded = all.map((c) => decodeChange(c))
      let encoded = decoded.map((c) => encodeChange(c))
      let doc2 = create();
      doc2.applyChanges(encoded)

      assert.deepStrictEqual(doc.spans(list) , doc2.spans(list))
      assert.deepStrictEqual(doc.save(), doc2.save())
    })
  })
})
