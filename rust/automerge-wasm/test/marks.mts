import { describe, it } from 'mocha';
//@ts-ignore
import assert from 'assert'
//@ts-ignore
import { create, load, Automerge, encodeChange, decodeChange } from '../nodejs/automerge_wasm.cjs'
import { v4 as uuid } from "uuid"

describe('Automerge', () => {
  describe('marks', () => {
    it('should handle marks [..]', () => {
      let doc = create()
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, { start: 3, end: 6, expand: "none" } , "bold" , true)
      let text = doc.text(list)
      let marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 3, end: 6 }])
      doc.insert(list, 6, "A")
      doc.insert(list, 3, "A")
      marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 4, end: 7 }])
    })

    it('should handle mark and unmark', () => {
      let doc = create()
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, { start: 2, end: 8 }, "bold" , true)
      let marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 2, end: 8 }])
      doc.unmark(list, { start: 4, end: 6, expand: 'none' }, 'bold')
      doc.insert(list, 7, "A")
      doc.insert(list, 3, "A")
      marks = doc.marks(list);
      assert.deepStrictEqual(marks, [
        { name: 'bold', value: true, start: 2, end: 5 },
        { name: 'bold', value: true, start: 7, end: 10 },
      ])
    })

    it('should handle mark and unmark of overlapping marks', () => {
      let doc = create()
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, { start: 2, end: 6 }, "bold" , true)
      doc.mark(list, { start: 5, end: 8 }, "bold" , true)
      doc.mark(list, { start: 3, end: 6 }, "underline" , true)
      let marks = doc.marks(list);
      assert.deepStrictEqual(marks, [
        { name: 'bold', value: true, start: 2, end: 8 },
        { name: 'underline', value: true, start: 3, end: 6 },
      ])
      doc.unmark(list, { start: 4, end: 6 }, 'bold')
      doc.insert(list, 7, "A")
      doc.insert(list, 3, "A")
      marks = doc.marks(list);
      assert.deepStrictEqual(marks, [
        { name: 'bold', value: true, start: 2, end: 5 },
        { name: 'bold', value: true, start: 7, end: 10 },
        { name: 'underline', value: true, start: 4, end: 7 },
      ])
      doc.unmark(list, { start: 0, end: 11 }, 'bold')
      marks = doc.marks(list);
      assert.deepStrictEqual(marks, [
        { name: 'underline', value: true, start: 4, end: 7 }
      ])
    })

    it('should handle marks [..] at the beginning of a string', () => {
      let doc = create()
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, { start: 0, end: 3, expand: "none" }, "bold", true)
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
      let doc = create()
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, { start: 0, end: 3, expand: "none" }, "bold", true)
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
      let doc = create()
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, { start: 0, end: 3 }, "bold", true)
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
      let doc = create()
      let list = doc.putObject("_root", "list", "")

      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, { start: 3, end: 6, expand: "none" }, "bold" , true)
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

    it('should handle expand marks (..)', () => {
      let doc = create()
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, { start: 3, end: 6, expand: "both" }, "bold" , true)
      let marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 3, end: 6 }])
      doc.insert(list, 6, "A")
      doc.insert(list, 3, "A")
      marks = doc.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 3, end: 8 }])
    })

    it('should handle expand marks with deleted ends (..)', () => {
      let doc = create()
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "aaabbbccc")
      doc.mark(list, { start: 3, end: 6, expand: "both" }, "bold" , true)
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
      let doc2 = load(saved)
      marks = doc2.marks(list);
      assert.deepStrictEqual(marks, [{ name: 'bold', value: true, start: 2, end: 5 }])

      assert.deepStrictEqual(doc.getHeads(), doc2.getHeads())
      assert.deepStrictEqual(doc.save(), doc2.save())
    })

    it('should handle overlapping marks', () => {
      let doc : Automerge = create({ actor: "aabbcc" })
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "the quick fox jumps over the lazy dog")
      doc.mark(list, { start: 0, end: 37 }, "bold" , true)
      doc.mark(list, { start: 4, end: 19 }, "itallic" , true)
      let id = uuid(); // we want each comment to be unique so give it a unique id
      doc.mark(list, { start: 10, end: 13 }, `comment:${id}` , "foxes are my favorite animal!")
      doc.commit("marks");
      let marks = doc.marks(list);
      assert.deepStrictEqual(marks, [
        { name: 'bold', start: 0, end: 37, value: true },
        { name: `comment:${id}`, start: 10, end: 13,  value: 'foxes are my favorite animal!' },
        { name: 'itallic', start: 4, end: 19, value: true },
      ])
      let text = doc.text(list);
      assert.deepStrictEqual(text, "the quick fox jumps over the lazy dog");

      let all = doc.getChanges([])
      let decoded = all.map((c) => decodeChange(c))
      let encoded = decoded.map((c) => encodeChange(c))
      let decoded2 = encoded.map((c) => decodeChange(c))
      let doc2 = create();
      doc2.applyChanges(encoded)

      assert.deepStrictEqual(doc.marks(list) , doc2.marks(list))
      assert.deepStrictEqual(doc.save(), doc2.save())
    })

    it('generates patches for marks made locally', () => {
      let doc : Automerge = create({ actor:"aabbcc" })
      let list = doc.putObject("_root", "list", "")
      doc.splice(list, 0, 0, "the quick fox jumps over the lazy dog")
      let h1 = doc.getHeads()
      doc.mark(list, { start: 0, end: 37 }, "bold" , true)
      doc.mark(list, { start: 4, end: 19 }, "itallic" , true)
      let id = uuid(); // we want each comment to be unique so give it a unique id
      doc.mark(list, { start: 10, end: 13 }, `comment:${id}` , "foxes are my favorite animal!")
      doc.commit("marks");
      let h2 = doc.getHeads()
      let patches = doc.diffIncremental();
      assert.deepEqual(patches, [
        { action: 'put', path: [ 'list' ], value: '' },
        {
          action: 'splice', path: [ 'list', 0 ],
          value: 'the ',
          marks: { bold: true },
        },
        {
          action: 'splice', path: [ 'list', 4 ],
          value: 'quick ',
          marks: { bold: true, itallic: true },
        },
        {
          action: 'splice', path: [ 'list', 10 ],
          value: 'fox',
          marks: {
            bold: true,
            [`comment:${id}`]: "foxes are my favorite animal!",
            itallic: true,
          }
        },
        {
          action: 'splice', path: [ 'list', 13 ],
          value: ' jumps',
          marks: { bold: true, itallic: true },
        },
        {
          action: 'splice', path: [ 'list', 19 ],
          value: ' over the lazy dog',
          marks: { bold: true },
        }
      ]);
    })

    it('marks should create patches that respect marks that supersede it', () => {

      let doc1 : Automerge = create({ actor: "aabbcc"})
      let list = doc1.putObject("_root", "list", "")
      doc1.splice(list, 0, 0, "the quick fox jumps over the lazy dog")

      let doc2 = load(doc1.save());

      let doc3 = load(doc1.save());

      doc1.put("/","foo", "bar"); // make a change to our op counter is higher than doc2
      doc1.mark(list, { start: 0, end: 5 }, "x", "a")
      doc1.mark(list, { start: 8, end: 11 }, "x", "b")

      doc2.mark(list, { start: 4, end: 13 }, "x", "c");

      doc3.updateDiffCursor();
      doc3.merge(doc1)
      doc3.merge(doc2)

      let patches = doc3.diffIncremental();

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
      let doc1 : Automerge = create({ actor: "aabbcc"})

      let list = doc1.putObject("_root", "list", "")
      doc1.splice(list, 0, 0, "the quick fox jumps over the lazy dog")
      doc1.mark(list, { start: 5, end: 10 }, "xxx", "aaa")

      let patches1 = doc1.diffIncremental().filter((p:any) => p.marks || p.action == 'mark')

      assert.deepEqual(patches1, [{
        action: 'splice', path: [ 'list', 5 ], marks: { xxx: 'aaa' }, value: "uick "
      }]);

      let doc2 : Automerge = create();
      doc2.loadIncremental(doc1.save())

      let patches2 = doc2.diffIncremental().filter((p:any) => p.marks || p.action == 'mark')

      assert.deepEqual(patches2, [{
        action: 'splice', path: [ 'list', 5 ], marks: { xxx: 'aaa' }, value: "uick "
      }]);
    })

    it('a overlapping marks will coalesse on load', () => {
      let doc1 : Automerge = create({ actor: "aabbcc" })

      let list = doc1.putObject("_root", "list", "")
      doc1.splice(list, 0, 0, "the quick fox jumps over the lazy dog")
      doc1.mark(list, { start: 5, end: 15 }, "xxx", "aaa")
      doc1.mark(list, { start: 10, end: 20 }, "xxx", "aaa")
      doc1.mark(list, { start: 15, end: 25 }, "xxx", "aaa")

      let patches1 = doc1.diffIncremental().filter((p:any) => p.marks || p.action == 'mark')

      assert.deepEqual(patches1, [
        { action: 'splice', path: [ 'list', 5 ], marks: { xxx: 'aaa' }, value: "uick fox jumps over "},
      ]);

      let doc2 : Automerge = create();
      doc2.loadIncremental(doc1.save())

      let patches2 = doc2.diffIncremental().filter((p:any) => p.marks || p.action == 'mark')

      assert.deepEqual(patches2, [
        { action: 'splice', path: [ 'list', 5 ], marks: { xxx: 'aaa' }, value: "uick fox jumps over "},
      ]);
    })

    it('coalesse handles different values', () => {
      let doc1 : Automerge = create({ actor: "aabbcc"})

      let list = doc1.putObject("_root", "list", "")
      doc1.splice(list, 0, 0, "the quick fox jumps over the lazy dog")
      doc1.mark(list, { start: 5, end: 15 }, "xxx", "aaa")
      doc1.mark(list, { start: 10, end: 20 }, "xxx", "bbb")
      doc1.mark(list, { start: 15, end: 25 }, "xxx", "aaa")

      let patches1 = doc1.diffIncremental().filter((p:any) => p.marks || p.action == 'mark')

      assert.deepEqual(patches1, [
        { action: 'splice', path: [ 'list', 5 ], marks: { xxx: 'aaa' }, value: "uick "},
        { action: 'splice', path: [ 'list', 10 ], marks: { xxx: 'bbb' }, value: "fox j"},
        { action: 'splice', path: [ 'list', 15 ], marks: { xxx: 'aaa' }, value: "umps over "},
      ]);

      let doc2 : Automerge = create();
      doc2.loadIncremental(doc1.save())

      let patches2 = doc2.diffIncremental().filter((p:any) => p.marks || p.action == 'mark')

      assert.deepEqual(patches2, [
        { action: 'splice', path: [ 'list', 5 ], marks: { xxx: 'aaa' }, value: "uick "},
        { action: 'splice', path: [ 'list', 10 ], marks: { xxx: 'bbb' }, value: "fox j"},
        { action: 'splice', path: [ 'list', 15 ], marks: { xxx: 'aaa' }, value: "umps over "},
      ]);
    })

    it('wont coalesse handles different names', () => {
      let doc1 : Automerge = create({ actor: "aabbcc" })

      let list = doc1.putObject("_root", "list", "")
      doc1.splice(list, 0, 0, "the quick fox jumps over the lazy dog")
      doc1.mark(list, { start: 5, end: 15 }, "xxx", "aaa")
      doc1.mark(list, { start: 10, end: 20 }, "yyy", "aaa")
      doc1.mark(list, { start: 15, end: 25 }, "zzz", "aaa")

      let patches1 = doc1.diffIncremental().filter((p:any) => p.marks || p.action == 'mark')

      assert.deepEqual(patches1, [
        { action: 'splice', path: [ 'list', 5 ], marks: { xxx: 'aaa' }, value: "uick "},
        { action: 'splice', path: [ 'list', 10 ], marks: { xxx: 'aaa', yyy: "aaa" }, value: "fox j"},
        { action: 'splice', path: [ 'list', 15 ], marks: { yyy: "aaa", zzz: "aaa" }, value: "umps "},
        { action: 'splice', path: [ 'list', 20 ], marks: { zzz: "aaa" }, value: "over "},
      ]);

      let doc2 : Automerge = create();
      doc2.loadIncremental(doc1.save())

      let patches2 = doc2.diffIncremental().filter((p:any) => p.marks || p.action == 'mark')

      assert.deepEqual(patches2, [
        { action: 'splice', path: [ 'list', 5 ], marks: { xxx: 'aaa' }, value: "uick "},
        { action: 'splice', path: [ 'list', 10 ], marks: { xxx: 'aaa', yyy: "aaa" }, value: "fox j"},
        { action: 'splice', path: [ 'list', 15 ], marks: { yyy: "aaa", zzz: "aaa" }, value: "umps "},
        { action: 'splice', path: [ 'list', 20 ], marks: { zzz: "aaa" }, value: "over "},
      ]);
    })

    it('coalesse handles async merge', () => {
      let doc1 : Automerge = create({ actor: "aabbcc" })

      let list = doc1.putObject("_root", "list", "")
      doc1.splice(list, 0, 0, "the quick fox jumps over the lazy dog")

      let doc2 = doc1.fork()

      doc1.put("/", "key1", "value"); // incrementing op counter so we win vs doc2
      doc1.put("/", "key2", "value"); // incrementing op counter so we win vs doc2
      doc1.mark(list, { start: 10, end: 20 }, "xxx", "aaa")
      doc1.mark(list, { start: 15, end: 25 }, "xxx", "aaa")

      doc2.mark(list, { start: 5, end: 30 }, "xxx", "bbb")

      doc1.merge(doc2)

      let patches1 = doc1.diffIncremental().filter((p:any) => p.marks || p.action == 'mark')

      assert.deepEqual(patches1, [
        { action: 'splice', path: [ 'list', 5 ], marks: { xxx: 'bbb' }, value: "uick "},
        { action: 'splice', path: [ 'list', 10 ], marks: { xxx: 'aaa' }, value: "fox jumps over "},
        { action: 'splice', path: [ 'list', 25 ], marks: { xxx: 'bbb' }, value: "the l"},
      ]);

      let doc3 : Automerge = create();
      doc3.loadIncremental(doc1.save())

      let patches2 = doc3.diffIncremental().filter((p:any) => p.marks || p.action == 'mark')

      let marks = doc3.marks(list)

      assert.deepEqual(marks, [
        { end: 10, name: "xxx", start: 5, value: "bbb" },
        { end: 25, name: "xxx", start: 10, value: "aaa" },
        { end: 30, name: "xxx", start: 25, value: "bbb" },
      ]);

      assert.deepEqual(patches2, [
        { action: 'splice', path: [ 'list', 5 ], marks: { xxx: 'bbb' }, value: "uick "},
        { action: 'splice', path: [ 'list', 10 ], marks: { xxx: 'aaa' }, value: "fox jumps over "},
        { action: 'splice', path: [ 'list', 25 ], marks: { xxx: 'bbb' }, value: "the l"},
      ])
    })

    it('does not show marks hidden in merge', () => {
      let doc1 : Automerge = create({ actor: "aabbcc" })

      let list = doc1.putObject("_root", "list", "")
      doc1.splice(list, 0, 0, "the quick fox jumps over the lazy dog")

      let doc2 = doc1.fork()

      doc1.put("/", "key1", "value"); // incrementing op counter so we win vs doc2
      doc1.put("/", "key2", "value"); // incrementing op counter so we win vs doc2
      doc1.mark(list, { start: 10, end: 20 }, "xxx", "aaa")
      doc1.mark(list, { start: 15, end: 25 }, "xxx", "aaa")

      doc2.mark(list, { start: 11, end: 24 }, "xxx", "bbb")

      doc1.merge(doc2)

      let patches1 = doc1.diffIncremental().filter((p:any) => p.marks)

      assert.deepEqual(patches1, [
        { action: 'splice', path: [ 'list', 10 ], marks: { xxx: 'aaa' }, value: "fox jumps over "},
      ]);

      let doc3 : Automerge = create();
      doc3.loadIncremental(doc1.save())

      let patches2 = doc3.diffIncremental().filter((p:any) => p.marks)

      assert.deepEqual(patches2, [
        { action: 'splice', path: [ 'list', 10 ], marks: { xxx: 'aaa' }, value: "fox jumps over "},
      ]);
    })

    it('coalesse disconnected marks with async merge', () => {
      let doc1 : Automerge = create({ actor: "aabbcc" })

      let list = doc1.putObject("_root", "list", "")
      doc1.splice(list, 0, 0, "the quick fox jumps over the lazy dog")

      let doc2 = doc1.fork()

      doc1.put("/", "key1", "value"); // incrementing op counter so we win vs doc2
      doc1.put("/", "key2", "value"); // incrementing op counter so we win vs doc2
      doc1.mark(list, { start: 5, end: 11 }, "xxx", "aaa")
      doc1.mark(list, { start: 19, end: 25 }, "xxx", "aaa")

      doc2.mark(list, { start: 10, end: 20 }, "xxx", "aaa")

      doc1.merge(doc2)

      let patches1 = doc1.diffIncremental().filter((p:any) => p.marks)

      assert.deepEqual(patches1, [
        { action: 'splice', path: [ 'list', 5 ], marks: { xxx: 'aaa' }, value: "uick fox jumps over "},
      ]);

      let doc3 : Automerge = create();
      doc3.loadIncremental(doc1.save())

      let patches2 = doc3.diffIncremental().filter((p:any) => p.marks)

      assert.deepEqual(patches2, [
        { action: 'splice', path: [ 'list', 5 ], marks: { xxx: 'aaa' }, value: "uick fox jumps over "},
      ]);
    })

    it('can get marks at a given heads', () => {
      let doc1 : Automerge = create({ actor: "aabbcc" })

      let list = doc1.putObject("_root", "list", "")
      doc1.splice(list, 0, 0, "the quick fox jumps over the lazy dog")

      let heads1 = doc1.getHeads();
      let marks1 = doc1.marks(list);

      doc1.mark(list, { start: 3, end: 25 }, "xxx", "aaa")

      let heads2 = doc1.getHeads();
      let marks2 = doc1.marks(list);

      doc1.mark(list, { start: 4, end: 11 }, "yyy", "bbb")

      let heads3 = doc1.getHeads();
      let marks3 = doc1.marks(list);

      doc1.unmark(list, { start: 9, end: 20 }, "xxx")

      let heads4 = doc1.getHeads();
      let marks4 = doc1.marks(list);

      assert.deepEqual(marks1, doc1.marks(list,heads1))
      assert.deepEqual(marks2, doc1.marks(list,heads2))
      assert.deepEqual(marks3, doc1.marks(list,heads3))
      assert.deepEqual(marks4, doc1.marks(list,heads4))
    })

    it('patches for marks generate correctly on load, on merge, and on change', () => {
      let doc1 : Automerge = create()

      let text = doc1.putObject("_root", "text", "")

      let heads0 = doc1.getHeads();

      doc1.splice(text, 0, 0, "aaaaabbbbbcccccdddddeeeeeffff")

      let heads1 = doc1.getHeads();

      doc1.updateDiffCursor();

      doc1.mark(text, { start: 5, end: 25 }, "mark1", "A")
      doc1.mark(text, { start: 10, end: 25 }, "mark2", "B")
      doc1.mark(text, { start: 15, end: 20 }, "mark2", "C")

      let patches1 = doc1.diffIncremental();
      let marks1 = doc1.marks(text);

      assert.deepEqual(marks1, [
          { start: 5, end: 25, name: 'mark1', value: 'A' },
          { start: 10, end: 15, name: 'mark2', value: 'B' },
          { start: 15, end: 20, name: 'mark2', value: 'C' },
          { start: 20, end: 25, name: 'mark2', value: 'B' },
      ])

      assert.deepEqual(patches1, [
        { action: 'mark', path: ['text'], marks: [
          { end: 25, name: 'mark1', start: 5, value: 'A' },
          { end: 25, name: 'mark2', start: 10, value: 'B' },
          { end: 20, name: 'mark2', start: 15, value: 'C' },
        ]}
      ]);

      let doc2 = load(doc1.save())
      let patches2 = doc2.diffIncremental();
      // this should run current_state since the doc was empty
      assert.deepEqual(patches2, [
        { action: 'put', path: [ 'text' ], value: '' },
        { action: 'splice', path: [ 'text', 0 ], value: 'aaaaa' },
        { action: 'splice', marks: { mark1: 'A' }, path: [ 'text', 5 ], value: 'bbbbb' },
        { action: 'splice', marks: { mark1: 'A', mark2: 'B' }, path: [ 'text', 10 ], value: 'ccccc' },
        { action: 'splice', marks: { mark1: 'A', mark2: 'C' }, path: [ 'text', 15 ], value: 'ddddd' },
        { action: 'splice', marks: { mark1: 'A', mark2: 'B' }, path: [ 'text', 20 ], value: 'eeeee' },
        { action: 'splice', path: [ 'text', 25 ], value: 'ffff' }
      ]);

      let doc3 = create();
      doc3.put("/", "a", "b"); // make a small change so we don't run current_state
      let headsLocal = doc3.getHeads()
      doc3.updateDiffCursor();
      doc3.merge(doc1)
      let patches3 = doc3.diffIncremental();
      assert.deepEqual(patches3, [
        { action: 'put', path: [ 'text' ], value: '' },
        { action: 'splice', path: [ 'text', 0 ], value: 'aaaaa' },
        { action: 'splice', path: [ 'text', 5 ], value: 'bbbbb', marks: { mark1: 'A' } },
        { action: 'splice', path: [ 'text', 10 ], value: 'ccccc', marks: { mark1: 'A', mark2: 'B' } },
        { action: 'splice', path: [ 'text', 15 ], value: 'ddddd', marks: { mark1: 'A', mark2: 'C' } },
        { action: 'splice', path: [ 'text', 20 ], value: 'eeeee', marks: { mark1: 'A', mark2: 'B' } },
        { action: 'splice', path: [ 'text', 25 ], value: 'ffff' }
      ]);

      let headsABPlusTextObj = [ ... heads0, ... headsLocal ];
      let patches4 = doc3.diff(doc3.getHeads(), headsABPlusTextObj);
      assert.deepEqual(patches4, [
        { action: 'del', length: 29, path: [ 'text', 0 ] }
      ]);
    })

    it('fully deleted marks will not attach to new text', () => {
      let doc1 : Automerge = create({ actor: "aabbcc" })

      let text = doc1.putObject("_root", "text", "The Peritext editor")
      doc1.mark(text, { start: 4, end: 12, expand: 'none' }, "link", true);
      doc1.mark(text, { start: 8, end: 12, expand: 'both' }, "bold", true);
      doc1.splice(text, 3, 10, "")
      doc1.splice(text, 3, 0, "!")

      let textval = doc1.text(text);
      let marks = doc1.marks(text);

      assert.deepEqual(doc1.text(text), "The!editor")
      assert.deepEqual(doc1.marks(text), [])
    })

    it('markAt() can be used to read the marks at a given index', () => {
      let doc = create()
      let text = doc.putObject("_root", "text", "aabbcc")

      doc.mark(text, { start: 0, end: 4, expand: "both" }, "bold" , true)
      doc.mark(text, { start: 2, end: 4, expand: "both" }, "underline" , true)

      doc.splice(text, 4, 0, ">")
      doc.splice(text, 2, 0, "<")

      assert.deepEqual(doc.marksAt(text, 0), { "bold": true })
      assert.deepEqual(doc.marksAt(text, 1), { "bold": true })

      assert.deepEqual(doc.marksAt(text, 2), { "bold": true, "underline": true })
      assert.deepEqual(doc.marksAt(text, 3), { "bold": true, "underline": true })
      assert.deepEqual(doc.marksAt(text, 4), { "bold": true, "underline": true })
      assert.deepEqual(doc.marksAt(text, 5), { "bold": true, "underline": true })

      assert.deepEqual(doc.marksAt(text, 6), { })
      assert.deepEqual(doc.marksAt(text, 7), { })
    })
  })
})
