import { describe, it } from 'mocha';
//@ts-ignore
import assert from 'assert'
//@ts-ignore
import { create, loadDoc, Automerge, encodeChange, decodeChange } from '..'

describe('Automerge', () => {
  describe('marks', () => {
    it('should handle marks [..]', () => {
      let doc = create()
      let list = doc.set_object("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "[3..6]", "bold" , true)
      let spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaa', [ [ 'bold', 'boolean', true ] ], 'bbb', [], 'ccc' ]);
      doc.insert(list, 6, "A")
      doc.insert(list, 3, "A")
      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaaA', [ [ 'bold', 'boolean', true ] ], 'bbb', [], 'Accc' ]);
    })

    it('should handle marks [..] at the beginning of a string', () => {
      let doc = create()
      let list = doc.set_object("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "[0..3]", "bold", true)
      let spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ [ [ 'bold', 'boolean', true ] ], 'aaa', [], 'bbbccc' ]);

      let doc2 = doc.fork()
      doc2.insert(list, 0, "A")
      doc2.insert(list, 4, "B")
      doc.merge(doc2)
      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'A', [ [ 'bold', 'boolean', true ] ], 'aaa', [], 'Bbbbccc' ]);
    })

    it('should handle marks [..] with splice', () => {
      let doc = create()
      let list = doc.set_object("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "[0..3]", "bold", true)
      let spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ [ [ 'bold', 'boolean', true ] ], 'aaa', [], 'bbbccc' ]);

      let doc2 = doc.fork()
      doc2.splice(list, 0, 2, "AAA")
      doc2.splice(list, 4, 0, "BBB")
      doc.merge(doc2)
      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'AAA', [ [ 'bold', 'boolean', true ] ], 'a', [], 'BBBbbbccc' ]);
    })

    it('should handle marks across multiple forks', () => {
      let doc = create()
      let list = doc.set_object("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "[0..3]", "bold", true)
      let spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ [ [ 'bold', 'boolean', true ] ], 'aaa', [], 'bbbccc' ]);

      let doc2 = doc.fork()
      doc2.splice(list, 1, 1, "Z") // replace 'aaa' with 'aZa' inside mark.

      let doc3 = doc.fork()
      doc3.insert(list, 0, "AAA") // should not be included in mark.

      doc.merge(doc2)
      doc.merge(doc3)

      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'AAA', [ [ 'bold', 'boolean', true ] ], 'aZa', [], 'bbbccc' ]);
    })


    it('should handle marks with deleted ends [..]', () => {
      let doc = create()
      let list = doc.set_object("_root", "list", "")

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
      let list = doc.set_object("_root", "list", "")
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
      let list = doc.set_object("_root", "list", "")
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
      let list = doc.set_object("_root", "list", "")
      doc.splice(list, 0, 0, "the quick fox jumps over the lazy dog")
      doc.mark(list, "[0..37]", "bold" , true)
      doc.mark(list, "[4..19]", "itallic" , true)
      doc.mark(list, "[10..13]", "comment" , "foxes are my favorite animal!")
      doc.commit("marks");
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
          { id: "39@aabbcc", start: 0, end: 37, type: 'bold', value: true },
          { id: "41@aabbcc", start: 4, end: 19, type: 'itallic', value: true },
          { id: "43@aabbcc", start: 10, end: 13, type: 'comment', value: 'foxes are my favorite animal!' }
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
