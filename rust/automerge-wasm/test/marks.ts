import { describe, it } from 'mocha';
//@ts-ignore
import assert from 'assert'
//@ts-ignore
import { create, load, Automerge, encodeChange, decodeChange } from '..'
import { v4 as uuid } from "uuid"


let util = require('util')

describe('Automerge', () => {
  describe('marks', () => {
    it('should handle marks [..]', () => {
      let doc = create(true)
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "[3..6]", "bold" , true)
      let text = doc.text(list)
      let marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 3, end: 6 }])
      doc.insert(list, 6, "A")
      doc.insert(list, 3, "A")
      marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 4, end: 7 }])
    })

    it('should handle mark and unmark', () => {
      let doc = create(true)
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "[2..8]", "bold" , true)
      let marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 2, end: 8 }])
      doc.unmark(list, 'bold', 4, 6)
      doc.insert(list, 7, "A")
      doc.insert(list, 3, "A")
      marks = doc.marks(list);
      assert.deepStrictEqual(marks, [
        { name: 'bold', value: true, start: 2, end: 5 },
        { name: 'bold', value: true, start: 7, end: 10 },
      ])
    })

    it('should handle mark and unmark of overlapping marks', () => {
      let doc = create(true)
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "[2..6]", "bold" , true)
      doc.mark(list, "[5..8]", "bold" , true)
      doc.mark(list, "[3..6]", "underline" , true)
      let marks = doc.marks(list);
      assert.deepStrictEqual(marks, [
        { name: 'underline', value: true, start: 3, end: 6 },
        { name: 'bold', value: true, start: 2, end: 8 },
      ])
      doc.unmark(list, 'bold', 4, 6)
      doc.insert(list, 7, "A")
      doc.insert(list, 3, "A")
      marks = doc.marks(list);
      assert.deepStrictEqual(marks, [
        { name: 'bold', value: true, start: 2, end: 5 },
        { name: 'underline', value: true, start: 4, end: 7 },
        { name: 'bold', value: true, start: 7, end: 10 },
      ])
      doc.unmark(list, 'bold', 0, 11)
      marks = doc.marks(list);
      assert.deepStrictEqual(marks, [
        { name: 'underline', value: true, start: 4, end: 7 }
      ])
    })

    it('should handle marks [..] at the beginning of a string', () => {
      let doc = create(true)
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "[0..3]", "bold", true)
      let marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 0, end: 3 }])

      let doc2 = doc.fork()
      doc2.insert(list, 0, "A")
      doc2.insert(list, 4, "B")
      doc.merge(doc2)
      marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 1, end: 4 }])
    })

    it('should handle marks [..] with splice', () => {
      let doc = create(true)
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "[0..3]", "bold", true)
      let marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 0, end: 3 }])

      let doc2 = doc.fork()
      doc2.splice(list, 0, 2, "AAA")
      doc2.splice(list, 4, 0, "BBB")
      doc.merge(doc2)
      marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 3, end: 4 }])
    })

    it('should handle marks across multiple forks', () => {
      let doc = create(true)
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "[0..3]", "bold", true)
      let marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 0, end: 3 }])

      let doc2 = doc.fork()
      doc2.splice(list, 1, 1, "Z") // replace 'aaa' with 'aZa' inside mark.

      let doc3 = doc.fork()
      doc3.insert(list, 0, "AAA") // should not be included in mark.

      doc.merge(doc2)
      doc.merge(doc3)

      marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 3, end: 6 }])
    })

    it('should handle marks with deleted ends [..]', () => {
      let doc = create(true)
      let list = doc.putObject("_root", "list", "")

      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "[3..6]", "bold" , true)
      let marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 3, end: 6 }])
      doc.delete(list,5);
      doc.delete(list,5);
      doc.delete(list,2);
      doc.delete(list,2);
      marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 2, end: 3 }])
      doc.insert(list, 3, "A")
      doc.insert(list, 2, "A")
      marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 3, end: 4 }])
    })

    it('should handle sticky marks (..)', () => {
      let doc = create(true)
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "(3..6)", "bold" , true)
      let marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 3, end: 6 }])
      doc.insert(list, 6, "A")
      doc.insert(list, 3, "A")
      marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 3, end: 8 }])
    })

    it('should handle sticky marks with deleted ends (..)', () => {
      let doc = create(true)
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, "(3..6)", "bold" , true)
      let marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 3, end: 6 }])
      doc.delete(list,5);
      doc.delete(list,5);
      doc.delete(list,2);
      doc.delete(list,2);
      marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 2, end: 3 }])
      doc.insert(list, 3, "A")
      doc.insert(list, 2, "A")
      marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 2, end: 5 }])

      // make sure save/load can handle marks

      let saved = doc.save()
      let doc2 = load(saved,true)
      marks = doc2.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 2, end: 5 }])

      assert.deepStrictEqual(doc.getHeads(), doc2.getHeads())
      assert.deepStrictEqual(doc.save(), doc2.save())
    })

    it('should handle overlapping marks', () => {
      let doc : Automerge = create(true, "aabbcc")
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "the quick fox jumps over the lazy dog")
      doc.mark(list, "[0..37]", "bold" , true)
      doc.mark(list, "[4..19]", "itallic" , true)
      let id = uuid(); // we want each comment to be unique so give it a unique id
      doc.mark(list, "[10..13]", `comment:${id}` , "foxes are my favorite animal!")
      doc.commit("marks");
      let marks = doc.marks(list);
      assert.deepStrictEqual(marks, [
        { name: `comment:${id}`, start: 10, end: 13,  value: 'foxes are my favorite animal!' },
        { name: 'itallic', start: 4, end: 19, value: true },
        { name: 'bold', start: 0, end: 37, value: true }
      ])
      let text = doc.text(list);
      assert.deepStrictEqual(text, "the quick fox jumps over the lazy dog");

      let all = doc.getChanges([])
      let decoded = all.map((c) => decodeChange(c))
      let util = require('util');
      let encoded = decoded.map((c) => encodeChange(c))
      let decoded2 = encoded.map((c) => decodeChange(c))
      let doc2 = create(true);
      doc2.applyChanges(encoded)

      assert.deepStrictEqual(doc.marks(list) , doc2.marks(list))
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
      let id = uuid(); // we want each comment to be unique so give it a unique id
      doc.mark(list, "[10..13]", `comment:${id}` , "foxes are my favorite animal!")
      doc.commit("marks");
      let h2 = doc.getHeads()
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
            { name: 'bold', value: true, start: 0, end: 37  },
            { name: 'itallic', value: true, start: 4, end: 19 },
            { name: `comment:${id}`, value: 'foxes are my favorite animal!', start: 10, end: 13 }
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
              { name: 'x', value: 'a', start: 0, end: 5 },
              { name: 'x', value: 'b', start: 8, end: 11 },
              { name: 'x', value: 'c', start: 5, end: 8 },
              { name: 'x', value: 'c', start: 11, end: 13 },
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
        action: 'mark', path: [ 'list' ], marks: [ { name: 'xxx', value: 'aaa', start: 5, end: 10 }],
      }]);

      let doc2 : Automerge = create(true);
      doc2.enablePatches(true)
      doc2.loadIncremental(doc1.save())

      let patches2 = doc2.popPatches().filter((p:any) => p.action == "mark")

      assert.deepEqual(patches2, [{
        action: 'mark', path: ['list'], marks: [ { name: 'xxx', value: 'aaa', start: 5, end: 10}],
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
          { name: 'xxx', value: 'aaa', start: 5, end: 15 },
          { name: 'xxx', value: 'aaa', start: 10, end: 20 },
          { name: 'xxx', value: 'aaa', start: 15, end: 25 },
        ] },
      ]);

      let doc2 : Automerge = create(true);
      doc2.enablePatches(true)
      doc2.loadIncremental(doc1.save())

      let patches2 = doc2.popPatches().filter((p:any) => p.action == "mark")

      assert.deepEqual(patches2, [
        { action: 'mark', path: ['list'], marks: [ { name: 'xxx', value: 'aaa', start: 5, end: 25}] },
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
          { name: 'xxx', value: 'aaa', start: 5, end: 15 },
          { name: 'xxx', value: 'bbb', start: 10, end: 20 },
          { name: 'xxx', value: 'aaa', start: 15, end: 25 },
        ]}
      ]);

      let doc2 : Automerge = create(true);
      doc2.enablePatches(true)
      doc2.loadIncremental(doc1.save())

      let patches2 = doc2.popPatches().filter((p:any) => p.action == "mark")

      assert.deepEqual(patches2, [
        { action: 'mark', path: ['list'], marks: [
          { name: 'xxx', value: 'aaa', start: 5, end: 10 },
          { name: 'xxx', value: 'bbb', start: 10, end: 15 },
          { name: 'xxx', value: 'aaa', start: 15, end: 25 },
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
          { name: 'xxx', value: 'aaa', start: 5, end:15 },
          { name: 'yyy', value: 'aaa', start: 10, end: 20 },
          { name: 'zzz', value: 'aaa', start: 15, end: 25 },
          ]}
      ]);

      let doc2 : Automerge = create(true);
      doc2.enablePatches(true)
      doc2.loadIncremental(doc1.save())

      let patches2 = doc2.popPatches().filter((p:any) => p.action == "mark")

      assert.deepEqual(patches2, [
        { action: 'mark', path: [ 'list' ], marks: [
          { name: 'xxx', value: 'aaa', start: 5, end: 15 },
          { name: 'yyy', value: 'aaa', start: 10, end: 20 },
          { name: 'zzz', value: 'aaa', start: 15, end: 25 },
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
            { name: 'xxx', value: 'aaa', start: 10, end: 20 },
            { name: 'xxx', value: 'aaa', start: 15, end: 25 },
            { name: 'xxx', value: 'bbb', start: 5, end: 10 },
            { name: 'xxx', value: 'bbb', start: 25, end: 30 },
          ]
        },
      ]);

      let doc3 : Automerge = create(true);
      doc3.enablePatches(true)
      doc3.loadIncremental(doc1.save())

      let patches2 = doc3.popPatches().filter((p:any) => p.action == "mark")

      let marks = doc3.marks(list)

      assert.deepEqual(marks, [
          { name: 'xxx', value: 'bbb', start: 5, end: 10 },
          { name: 'xxx', value: 'aaa', start: 10, end: 25 },
          { name: 'xxx', value: 'bbb', start: 25, end: 30  },
      ]);

      assert.deepEqual(patches2, [{ action: 'mark', path: [ 'list' ], marks }]);
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
            { name: 'xxx', value: 'aaa', start: 10, end: 20 },
            { name: 'xxx', value: 'aaa', start: 15, end: 25 },
          ]
        },
      ]);

      let doc3 : Automerge = create(true);
      doc3.enablePatches(true)
      doc3.loadIncremental(doc1.save())

      let patches2 = doc3.popPatches().filter((p:any) => p.action == "mark")

      assert.deepEqual(patches2, [
        { action: 'mark', path: [ 'list' ], marks: [
          { name: 'xxx', value: 'aaa', start: 10, end: 25 },
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
            { name: 'xxx', value: 'aaa', start: 5, end: 11 },
            { name: 'xxx', value: 'aaa', start: 19, end: 25 },
            { name: 'xxx', value: 'aaa', start: 11, end: 19 },
          ]
        },
      ]);

      let doc3 : Automerge = create(true);
      doc3.enablePatches(true)
      doc3.loadIncremental(doc1.save())

      let patches2 = doc3.popPatches().filter((p:any) => p.action == "mark")

      assert.deepEqual(patches2, [
        { action: 'mark', path: [ 'list' ], marks: [
          { name: 'xxx', value: 'aaa', start: 5, end: 25 },
        ]}
      ]);
    })
    it('can get marks at a given heads', () => {
      let doc1 : Automerge = create(true, "aabbcc")
      doc1.enablePatches(true)

      let list = doc1.putObject("_root", "list", "")
      doc1.splice(list, 0, 0, "the quick fox jumps over the lazy dog")

      let heads1 = doc1.getHeads();
      let marks1 = doc1.marks(list);

      doc1.mark(list, "[3..25]", "xxx", "aaa")

      let heads2 = doc1.getHeads();
      let marks2 = doc1.marks(list);

      doc1.mark(list, "[4..11]", "yyy", "bbb")

      let heads3 = doc1.getHeads();
      let marks3 = doc1.marks(list);

      doc1.unmark(list, "xxx", 9, 20)

      let heads4 = doc1.getHeads();
      let marks4 = doc1.marks(list);

      assert.deepEqual(marks1, doc1.marks(list,heads1))
      assert.deepEqual(marks2, doc1.marks(list,heads2))
      assert.deepEqual(marks3, doc1.marks(list,heads3))
      assert.deepEqual(marks4, doc1.marks(list,heads4))
    })
  })
})
