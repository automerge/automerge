
import { describe, it } from 'mocha';
//@ts-ignore
import assert from 'assert'
//@ts-ignore
import init, { create, load } from '..'

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
      let mat = doc1.materialize("/")
      let base = doc1.applyPatches({})
      assert.deepEqual(mat, start)

      doc1.delete("/list/0/1", 3)
      start.list[0][1].splice(3,1)

      doc1.delete("/list/0", 0)
      start.list[0].splice(0,1)

      mat = doc1.materialize("/")
      base = doc1.applyPatches(base)
      assert.deepEqual(mat, start)
      assert.deepEqual(base, start)
    })
  })
})
