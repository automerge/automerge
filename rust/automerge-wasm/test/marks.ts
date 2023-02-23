import { describe, it } from 'mocha';
//@ts-ignore
import assert from 'assert'
//@ts-ignore
import { create, load, Automerge, encodeChange, decodeChange } from '..'

let util = require('util')

describe('Automerge', () => {
  describe('marks', () => {
    it('should handle marks [..]', () => {
      let doc = create(true)
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "[3..6]", "bold" , true)
      let text = doc.text(list)
      let spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaa', [ [ 'bold', 'boolean', true ] ], 'bbb', [], 'ccc' ]);
      doc.insert(list, 6, "A")
      doc.insert(list, 3, "A")
      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaaA', [ [ 'bold', 'boolean', true ] ], 'bbb', [], 'Accc' ]);
    })

    it('should handle marks [..] at the beginning of a string', () => {
      let doc = create(true)
      let list = doc.putObject("_root", "list", "")
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
      let doc = create(true)
      let list = doc.putObject("_root", "list", "")
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
      let doc = create(true)
      let list = doc.putObject("_root", "list", "")
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
      let doc = create(true)
      let list = doc.putObject("_root", "list", "")

      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "[3..6]", "bold" , true)
      let spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaa', [ [ 'bold', 'boolean', true ] ], 'bbb', [], 'ccc' ]);
      doc.delete(list,5);
      doc.delete(list,5);
      doc.delete(list,2);
      doc.delete(list,2);
      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aa', [ [ 'bold', 'boolean', true ] ], 'b', [], 'cc' ])
      doc.insert(list, 3, "A")
      doc.insert(list, 2, "A")
      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaA', [ [ 'bold', 'boolean', true ] ], 'b', [], 'Acc' ])
    })

    it('should handle sticky marks (..)', () => {
      let doc = create(true)
      let list = doc.putObject("_root", "list", "")
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
      let doc = create(true)
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "(3..6)", "bold" , true)
      let spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aaa', [ [ 'bold', 'boolean', true ] ], 'bbb', [], 'ccc' ]);
      doc.delete(list,5);
      doc.delete(list,5);
      doc.delete(list,2);
      doc.delete(list,2);
      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aa', [ [ 'bold', 'boolean', true ] ], 'b', [], 'cc' ])
      doc.insert(list, 3, "A")
      doc.insert(list, 2, "A")
      spans = doc.spans(list);
      assert.deepStrictEqual(spans, [ 'aa', [ [ 'bold', 'boolean', true ] ], 'AbA', [], 'cc' ])

      // make sure save/load can handle marks

      let saved = doc.save()
      let doc2 = load(saved,true)
      spans = doc2.spans(list);
      assert.deepStrictEqual(spans, [ 'aa', [ [ 'bold', 'boolean', true ] ], 'AbA', [], 'cc' ])

      assert.deepStrictEqual(doc.getHeads(), doc2.getHeads())
      assert.deepStrictEqual(doc.save(), doc2.save())
    })

    it('should handle overlapping marks', () => {
      let doc : Automerge = create(true, "aabbcc")
      let list = doc.putObject("_root", "list", "")
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
            [ 'itallic', 'boolean', true ],
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

      doc.unmark(list, "41@aabbcc")
      raw_spans = doc.raw_spans(list);
      assert.deepStrictEqual(raw_spans,
        [
          { id: "39@aabbcc", start: 0, end: 37, type: 'bold', value: true },
          { id: "43@aabbcc", start: 10, end: 13, type: 'comment', value: 'foxes are my favorite animal!' }
        ]);
      // mark sure encode decode can handle marks

      doc.unmark(list, "39@aabbcc")
      raw_spans = doc.raw_spans(list);
      assert.deepStrictEqual(raw_spans,
        [
          { id: "43@aabbcc", start: 10, end: 13, type: 'comment', value: 'foxes are my favorite animal!' }
        ]);

      let all = doc.getChanges([])
      let decoded = all.map((c) => decodeChange(c))
      let util = require('util');
      let encoded = decoded.map((c) => encodeChange(c))
      let decoded2 = encoded.map((c) => decodeChange(c))
      let doc2 = create(true);
      doc2.applyChanges(encoded)

      assert.deepStrictEqual(doc.spans(list) , doc2.spans(list))
      assert.deepStrictEqual(doc.save(), doc2.save())
    })

    it('generates patches for marks made locally', () => {
      let doc : Automerge = create(true, "aabbcc")
      doc.enablePatches(true)
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "the quick fox jumps over the lazy dog")
      let h1 = doc.getHeads()
      doc.mark(list, "[0..37]", "bold" , true)
      doc.mark(list, "[4..19]", "itallic" , true)
      doc.mark(list, "[10..13]", "comment" , "foxes are my favorite animal!")
      doc.commit("marks");
      let h2 = doc.getHeads()
      let x = doc.attribute2(list, [], [h2]);
      let patches = doc.popPatches();
      let util = require('util')
      assert.deepEqual(patches, [
        { action: 'put', path: [ 'list' ], value: '' },
        {
          action: 'splice', path: [ 'list', 0 ],
          value: 'the quick fox jumps over the lazy dog'
        },
        {
          action: 'mark', path: [ 'list' ],
          marks: [
            { name: 'bold', value: true, range: '0..37' },
            { name: 'itallic', value: true, range: '4..19' },
            { name: 'comment', value: 'foxes are my favorite animal!', range: '10..13' }
          ]
        }
      ]);
    })

    it('marks should create patches that respect marks that supersede it', () => {

      let doc1 : Automerge = create(true, "aabbcc")
      let list = doc1.putObject("_root", "list", "")
      doc1.splice(list, 0, 0, "the quick fox jumps over the lazy dog")

      let doc2 = load(doc1.save(),true);

      let doc3 = load(doc1.save(),true);
      doc3.enablePatches(true)

      doc1.put("/","foo", "bar"); // make a change to our op counter is higher than doc2
      doc1.mark(list, "[0..5]", "x", "a")
      doc1.mark(list, "[8..11]", "x", "b")

      doc2.mark(list, "[4..13]", "x", "c");

      doc3.merge(doc1)
      doc3.merge(doc2)

      let patches = doc3.popPatches();

      assert.deepEqual(patches, [
          { action: 'put', path: [ 'foo' ], value: 'bar' },
          {
            action: 'mark',
            path: [ 'list' ],
            marks: [
              { name: 'x', value: 'a', range: '0..5' },
              { name: 'x', value: 'b', range: '8..11' },
              { name: 'x', value: 'c', range: '5..8' },
              { name: 'x', value: 'c', range: '11..13' },
            ]
          },
        ]);
    })
  })
  describe('loading marks', () => {
    it('a mark will appear on load', () => {
      let doc1 : Automerge = create(true, "aabbcc")
      doc1.enablePatches(true)

      let list = doc1.putObject("_root", "list", "")
      doc1.splice(list, 0, 0, "the quick fox jumps over the lazy dog")
      doc1.mark(list, "[5..10]", "xxx", "aaa")

      let patches1 = doc1.popPatches().filter((p:any) => p.action == "mark")

      assert.deepEqual(patches1, [{
        action: 'mark', path: [ 'list' ], marks: [ { name: 'xxx', value: 'aaa', range: '5..10' }],
      }]);

      let doc2 : Automerge = create(true);
      doc2.enablePatches(true)
      doc2.loadIncremental(doc1.save())

      let patches2 = doc2.popPatches().filter((p:any) => p.action == "mark")

      assert.deepEqual(patches2, [{
        action: 'mark', path: ['list'], marks: [ { name: 'xxx', value: 'aaa', range: '5..10'}],
      }]);
    })

    it('a overlapping marks will coalesse on load', () => {
      let doc1 : Automerge = create(true, "aabbcc")
      doc1.enablePatches(true)

      let list = doc1.putObject("_root", "list", "")
      doc1.splice(list, 0, 0, "the quick fox jumps over the lazy dog")
      doc1.mark(list, "[5..15]", "xxx", "aaa")
      doc1.mark(list, "[10..20]", "xxx", "aaa")
      doc1.mark(list, "[15..25]", "xxx", "aaa")

      let patches1 = doc1.popPatches().filter((p:any) => p.action == "mark")

      assert.deepEqual(patches1, [
        { action: 'mark', path: [ 'list' ], marks: [
          { name: 'xxx', value: 'aaa', range: '5..15' },
          { name: 'xxx', value: 'aaa', range: '10..20' },
          { name: 'xxx', value: 'aaa', range: '15..25' },
        ] },
      ]);

      let doc2 : Automerge = create(true);
      doc2.enablePatches(true)
      doc2.loadIncremental(doc1.save())

      let patches2 = doc2.popPatches().filter((p:any) => p.action == "mark")

      assert.deepEqual(patches2, [
        { action: 'mark', path: ['list'], marks: [ { name: 'xxx', value: 'aaa', range: '5..25'}] },
      ]);
    })

    it('coalesse handles different values', () => {
      let doc1 : Automerge = create(true, "aabbcc")
      doc1.enablePatches(true)

      let list = doc1.putObject("_root", "list", "")
      doc1.splice(list, 0, 0, "the quick fox jumps over the lazy dog")
      doc1.mark(list, "[5..15]", "xxx", "aaa")
      doc1.mark(list, "[10..20]", "xxx", "bbb")
      doc1.mark(list, "[15..25]", "xxx", "aaa")

      let patches1 = doc1.popPatches().filter((p:any) => p.action == "mark")

      assert.deepEqual(patches1, [
        { action: 'mark', path: [ 'list' ], marks: [
          { name: 'xxx', value: 'aaa', range: '5..15' },
          { name: 'xxx', value: 'bbb', range: '10..20' },
          { name: 'xxx', value: 'aaa', range: '15..25' },
        ]}
      ]);

      let doc2 : Automerge = create(true);
      doc2.enablePatches(true)
      doc2.loadIncremental(doc1.save())

      let patches2 = doc2.popPatches().filter((p:any) => p.action == "mark")

      assert.deepEqual(patches2, [
        { action: 'mark', path: ['list'], marks: [
          { name: 'xxx', value: 'aaa', range: '5..10'},
          { name: 'xxx', value: 'bbb', range: '10..15'},
          { name: 'xxx', value: 'aaa', range: '15..25'},
        ]},
      ]);
    })

    it('wont coalesse handles different names', () => {
      let doc1 : Automerge = create(true, "aabbcc")
      doc1.enablePatches(true)

      let list = doc1.putObject("_root", "list", "")
      doc1.splice(list, 0, 0, "the quick fox jumps over the lazy dog")
      doc1.mark(list, "[5..15]", "xxx", "aaa")
      doc1.mark(list, "[10..20]", "yyy", "aaa")
      doc1.mark(list, "[15..25]", "zzz", "aaa")

      let patches1 = doc1.popPatches().filter((p:any) => p.action == "mark")

      assert.deepEqual(patches1, [
        { action: 'mark', path: [ 'list' ], marks: [
          { name: 'xxx', value: 'aaa', range: '5..15' },
          { name: 'yyy', value: 'aaa', range: '10..20' },
          { name: 'zzz', value: 'aaa', range: '15..25' },
          ]}
      ]);

      let doc2 : Automerge = create(true);
      doc2.enablePatches(true)
      doc2.loadIncremental(doc1.save())

      let patches2 = doc2.popPatches().filter((p:any) => p.action == "mark")

      assert.deepEqual(patches2, [
        { action: 'mark', path: [ 'list' ], marks: [
          { name: 'xxx', value: 'aaa', range: '5..15' },
          { name: 'yyy', value: 'aaa', range: '10..20' },
          { name: 'zzz', value: 'aaa', range: '15..25' },
        ]}
      ]);
    })

    it('coalesse handles async merge', () => {
      let doc1 : Automerge = create(true, "aabbcc")
      doc1.enablePatches(true)

      let list = doc1.putObject("_root", "list", "")
      doc1.splice(list, 0, 0, "the quick fox jumps over the lazy dog")

      let doc2 = doc1.fork()

      doc1.put("/", "key1", "value"); // incrementing op counter so we win vs doc2
      doc1.put("/", "key2", "value"); // incrementing op counter so we win vs doc2
      doc1.mark(list, "[10..20]", "xxx", "aaa")
      doc1.mark(list, "[15..25]", "xxx", "aaa")

      doc2.mark(list, "[5..30]" , "xxx", "bbb")

      doc1.merge(doc2)

      let patches1 = doc1.popPatches().filter((p:any) => p.action == "mark")

      assert.deepEqual(patches1, [
        { action: 'mark', path: [ 'list' ], marks: [
            { name: 'xxx', value: 'aaa', range: '10..20' },
            { name: 'xxx', value: 'aaa', range: '15..25' },
            { name: 'xxx', value: 'bbb', range: '5..10' },
            { name: 'xxx', value: 'bbb', range: '25..30' },
          ]
        },
      ]);

      let doc3 : Automerge = create(true);
      doc3.enablePatches(true)
      doc3.loadIncremental(doc1.save())

      let patches2 = doc3.popPatches().filter((p:any) => p.action == "mark")

      assert.deepEqual(patches2, [
        { action: 'mark', path: [ 'list' ], marks: [
          { name: 'xxx', value: 'bbb', range: '5..10' },
          { name: 'xxx', value: 'aaa', range: '10..25' },
          { name: 'xxx', value: 'bbb', range: '25..30' },
        ]}
      ]);
    })

    it('does not show marks hidden in merge', () => {
      let doc1 : Automerge = create(true, "aabbcc")
      doc1.enablePatches(true)

      let list = doc1.putObject("_root", "list", "")
      doc1.splice(list, 0, 0, "the quick fox jumps over the lazy dog")

      let doc2 = doc1.fork()

      doc1.put("/", "key1", "value"); // incrementing op counter so we win vs doc2
      doc1.put("/", "key2", "value"); // incrementing op counter so we win vs doc2
      doc1.mark(list, "[10..20]", "xxx", "aaa")
      doc1.mark(list, "[15..25]", "xxx", "aaa")

      doc2.mark(list, "[11..24]" , "xxx", "bbb")

      doc1.merge(doc2)

      let patches1 = doc1.popPatches().filter((p:any) => p.action == "mark")

      assert.deepEqual(patches1, [
        { action: 'mark', path: [ 'list' ], marks: [
            { name: 'xxx', value: 'aaa', range: '10..20' },
            { name: 'xxx', value: 'aaa', range: '15..25' },
          ]
        },
      ]);

      let doc3 : Automerge = create(true);
      doc3.enablePatches(true)
      doc3.loadIncremental(doc1.save())

      let patches2 = doc3.popPatches().filter((p:any) => p.action == "mark")

      assert.deepEqual(patches2, [
        { action: 'mark', path: [ 'list' ], marks: [
          { name: 'xxx', value: 'aaa', range: '10..25' },
        ]}
      ]);
    })

    it('coalesse disconnected marks with async merge', () => {
      let doc1 : Automerge = create(true, "aabbcc")
      doc1.enablePatches(true)

      let list = doc1.putObject("_root", "list", "")
      doc1.splice(list, 0, 0, "the quick fox jumps over the lazy dog")

      let doc2 = doc1.fork()

      doc1.put("/", "key1", "value"); // incrementing op counter so we win vs doc2
      doc1.put("/", "key2", "value"); // incrementing op counter so we win vs doc2
      doc1.mark(list, "[5..11]", "xxx", "aaa")
      doc1.mark(list, "[19..25]", "xxx", "aaa")

      doc2.mark(list, "[10..20]" , "xxx", "aaa")

      doc1.merge(doc2)

      let patches1 = doc1.popPatches().filter((p:any) => p.action == "mark")

      assert.deepEqual(patches1, [
        { action: 'mark', path: [ 'list' ], marks: [
            { name: 'xxx', value: 'aaa', range: '5..11' },
            { name: 'xxx', value: 'aaa', range: '19..25' },
            { name: 'xxx', value: 'aaa', range: '11..19' },
          ]
        },
      ]);

      let doc3 : Automerge = create(true);
      doc3.enablePatches(true)
      doc3.loadIncremental(doc1.save())

      let patches2 = doc3.popPatches().filter((p:any) => p.action == "mark")

      assert.deepEqual(patches2, [
        { action: 'mark', path: [ 'list' ], marks: [
          { name: 'xxx', value: 'aaa', range: '5..25' },
        ]}
      ]);
    })
  })
})
