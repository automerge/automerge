
import { describe, it } from 'mocha';
import assert from 'assert'
import { create } from '../nodejs/automerge_wasm.cjs'

export const OBJECT_ID  = Symbol.for('_am_objectId')     // object containing metadata about current 

// @ts-ignore
function _obj(doc: any) : any {
  if (typeof doc === 'object' && doc !== null) {
    return doc[OBJECT_ID]
  }
}

// sample classes for testing
class Counter {
  value: number;
  constructor(n: number) {
    this.value = n
  }
}

describe('Automerge', () => {
  describe('Patch Apply', () => {
    it('apply nested sets on maps', () => {
      const start = { hello: { mellow: { yellow: "world", x: 1 }, y : 2 } }
      const doc1 = create()
      doc1.putObject("/", "hello", start.hello);
      let mat = doc1.materialize("/")
      const doc2 = create()
      doc2.merge(doc1)

      let base = doc2.applyPatches({})
      assert.deepEqual(mat, start)
      assert.deepEqual(base, start)

      doc2.delete("/hello/mellow", "yellow");
      // @ts-ignore
      delete start.hello.mellow.yellow;
      base = doc2.applyPatches(base)
      mat = doc2.materialize("/")

      assert.deepEqual(mat, start)
      assert.deepEqual(base, start)
    })

    it('apply patches on lists', () => {
      const start = { list: [1,2,3,4] }
      const doc1 = create()
      doc1.putObject("/", "list", start.list);
      let mat = doc1.materialize("/")
      const doc2 = create()
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
      const start = { list:
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
      const doc1 = create()
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
      const doc1 = create()
      doc1.putObject("/", "list", "abc");
      const patches = doc1.diffIncremental()
      assert.deepEqual( patches, [
        { action: 'put', path: [ 'list' ], value: "" },
        { action: 'splice', path: [ 'list', 0 ], value: 'abc' }])
    })

    it('it should allow registering type wrappers', () => {
      const doc1 = create()
      doc1.registerDatatype("counter", (n: number) => new Counter(n), (c: any) => {
          if (c instanceof Counter) {
            return ["counter", c.value]
          }
      })
      const doc2 = doc1.fork()
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
      const doc1 = create({ actor: "aaaa" })

      doc1.putObject("/", "notes", "hello world")

      let mat = doc1.materialize("/")

      assert.deepEqual( mat, { notes: "hello world" } )

      const doc2 = create()
      let apply : any = doc2.materialize("/") 
      apply = doc2.applyPatches(apply)

      doc2.merge(doc1);
      apply = doc2.applyPatches(apply)
      assert.deepEqual(_obj(apply), "_root")
      assert.deepEqual( apply, { notes: "hello world" } )

      doc2.splice("/notes", 6, 5, "everyone");
      apply = doc2.applyPatches(apply)
      assert.deepEqual( apply, { notes: "hello everyone" } )

      mat = doc2.materialize("/")
      assert.deepEqual(_obj(mat), "_root")
      // @ts-ignore
      assert.deepEqual( mat, { notes: "hello everyone" } )
    })

    it('should set the OBJECT_ID property on lists, maps, and text objects and not on scalars', () => {
        const doc1 = create({ actor: 'aaaa' })
        const mat: any = doc1.materialize("/")
        doc1.registerDatatype("counter", (n: number) => new Counter(n), (c: any) => {
            if (c instanceof Counter) {
                return c.value
            } 
        })
        doc1.put("/", "string", "string", "str")
        doc1.put("/", "uint", 2, "uint")
        doc1.put("/", "int", 2, "int")
        doc1.put("/", "float", 2.3, "f64")
        doc1.put("/", "bytes", new Uint8Array(), "bytes")
        doc1.put("/", "counter", 1, "counter")
        doc1.put("/", "date", new Date(), "timestamp")
        doc1.putObject("/", "text", "text")
        doc1.putObject("/", "list", [])
        doc1.putObject("/", "map", {})
        const applied = doc1.applyPatches(mat)

        assert.equal(_obj(applied.string), null)
        assert.equal(_obj(applied.uint), null)
        assert.equal(_obj(applied.int), null)
        assert.equal(_obj(applied.float), null)
        assert.equal(_obj(applied.bytes), null)
        assert.equal(_obj(applied.counter), null)
        assert.equal(_obj(applied.date), null)
        assert.equal(_obj(applied.text), null)

        assert.notEqual(_obj(applied.list), null)
        assert.notEqual(_obj(applied.map), null)
    })

    it('should set the root OBJECT_ID to "_root"', () => {
        const doc1 = create({ actor: 'aaaa'})
        const mat: any = doc1.materialize("/")
        assert.equal(_obj(mat), "_root")
        doc1.put("/", "key", "value")
        const applied = doc1.applyPatches(mat)
        assert.equal(_obj(applied), "_root")
    })

    it.skip('it can patch quickly', () => {
/*
      console.time("init")
      let doc1 = create()
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
*/
    })
  })
})

// TODO: squash puts & deletes
