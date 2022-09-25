
import { describe, it } from 'mocha';
//@ts-ignore
import assert from 'assert'
//@ts-ignore
import init, { create, load } from '..'

export const OBJECT_ID  = Symbol.for('_am_objectId')     // object containing metadata about current 

// sample classes for testing
class Counter {
  value: number;
  constructor(n: number) {
    this.value = n
  }
}

class Wrapper {
  value: any;
  constructor(n: any) {
    this.value = n
  }
}

describe('Automerge', () => {
  describe('Patch Apply', () => {
    it('apply nested sets on maps', () => {
      let start : any = { hello: { mellow: { yellow: "world", x: 1 }, y : 2 } }
      let doc1 = create()
      doc1.putObject("/", "hello", start.hello);
      let mat = doc1.materialize("/")
      let doc2 = create()
      doc2.enablePatches(true)
      doc2.merge(doc1)

      let base = doc2.applyPatches({})
      assert.deepEqual(mat, start)
      assert.deepEqual(base, start)

      doc2.delete("/hello/mellow", "yellow");
      delete start.hello.mellow.yellow;
      base = doc2.applyPatches(base)
      mat = doc2.materialize("/")

      assert.deepEqual(mat, start)
      assert.deepEqual(base, start)
    })

    it('apply patches on lists', () => {
      //let start = { list: [1,2,3,4,5,6] }
      let start = { list: [1,2,3,4] }
      let doc1 = create()
      doc1.putObject("/", "list", start.list);
      let mat = doc1.materialize("/")
      let doc2 = create()
      doc2.enablePatches(true)
      doc2.merge(doc1)
      mat = doc1.materialize("/")
      let base = doc2.applyPatches({})
      assert.deepEqual(mat, start)
      assert.deepEqual(base, start)

      doc2.delete("/list", 3);
      start.list.splice(3,1)
      base = doc2.applyPatches(base)

      assert.deepEqual(base, start)
    })

    it('apply patches on lists of lists of lists', () => {
      let start = { list:
        [
          [
            [ 1, 2, 3, 4, 5, 6],
            [ 7, 8, 9,10,11,12],
          ],
          [
            [ 7, 8, 9,10,11,12],
            [ 1, 2, 3, 4, 5, 6],
          ]
        ]
      }
      let doc1 = create()
      doc1.enablePatches(true)
      doc1.putObject("/", "list", start.list);
      let base = doc1.applyPatches({})
      let mat = doc1.clone().materialize("/")
      assert.deepEqual(mat, start)
      assert.deepEqual(base, start)

      doc1.delete("/list/0/1", 3)
      start.list[0][1].splice(3,1)

      doc1.delete("/list/0", 0)
      start.list[0].splice(0,1)

      mat = doc1.clone().materialize("/")
      base = doc1.applyPatches(base)
      assert.deepEqual(mat, start)
      assert.deepEqual(base, start)
    })

    it('large inserts should make one splice patch', () => {
      let doc1 = create()
      doc1.enablePatches(true)
      doc1.putObject("/", "list", "abc");
      let patches = doc1.popPatches()
      assert.deepEqual( patches, [
        { action: 'put', conflict: false, path: [ 'list' ], value: [] },
        { action: 'splice', path: [ 'list', 0 ], values: [ 'a', 'b', 'c' ] }])
    })

    it('it should allow registering type wrappers', () => {
      let doc1 = create()
      doc1.enablePatches(true)
      //@ts-ignore
      doc1.registerDatatype("counter", (n: any) => new Counter(n))
      let doc2 = doc1.fork()
      doc1.put("/", "n", 10, "counter")
      doc1.put("/", "m", 10, "int")

      let mat = doc1.materialize("/")
      assert.deepEqual( mat, { n: new Counter(10), m: 10 } )

      doc2.merge(doc1)
      let apply = doc2.applyPatches({})
      assert.deepEqual( apply, { n: new Counter(10), m: 10 } )

      doc1.increment("/","n", 5)
      mat = doc1.materialize("/")
      assert.deepEqual( mat, { n: new Counter(15), m: 10 } )

      doc2.merge(doc1)
      apply = doc2.applyPatches(apply)
      assert.deepEqual( apply, { n: new Counter(15), m: 10 } )
    })

    it('text can be managed as an array or a string', () => {
      let doc1 = create("aaaa")
      doc1.enablePatches(true)

      doc1.putObject("/", "notes", "hello world")

      let mat = doc1.materialize("/")

      assert.deepEqual( mat, { notes: "hello world".split("") } )

      let doc2 = create()
      doc2.enablePatches(true)
      //@ts-ignore
      doc2.registerDatatype("text", (n: any[]) => new String(n.join("")))
      let apply = doc2.applyPatches({} as any)

      doc2.merge(doc1);
      apply = doc2.applyPatches(apply)
      assert.deepEqual(apply[OBJECT_ID], "_root")
      assert.deepEqual(apply.notes[OBJECT_ID], "1@aaaa")
      assert.deepEqual( apply, { notes: new String("hello world") } )

      doc2.splice("/notes", 6, 5, "everyone");
      apply = doc2.applyPatches(apply)
      assert.deepEqual( apply, { notes: new String("hello everyone") } )

      mat = doc2.materialize("/")
      //@ts-ignore
      assert.deepEqual(mat[OBJECT_ID], "_root")
      //@ts-ignore
      assert.deepEqual(mat.notes[OBJECT_ID], "1@aaaa")
      assert.deepEqual( mat, { notes: new String("hello everyone") } )
    })

    it.skip('it can patch quickly', () => {
      console.time("init")
      let doc1 = create()
      doc1.enablePatches(true)
      doc1.putObject("/", "notes", "");
      let mat = doc1.materialize("/")
      let doc2 = doc1.fork()
      let testData = new Array( 100000 ).join("x")
      console.timeEnd("init")
      console.time("splice")
      doc2.splice("/notes", 0, 0, testData);
      console.timeEnd("splice")
      console.time("merge")
      doc1.merge(doc2)
      console.timeEnd("merge")
      console.time("patch")
      mat = doc1.applyPatches(mat)
      console.timeEnd("patch")
    })
  })
})

// TODO: squash puts & deletes
