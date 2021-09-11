
const assert = require('assert')
const Automerge = require('..')

describe('Automerge', () => {
  describe('basics', () => {
    it('should init clone and free', () => {
      let doc1 = Automerge.init()
      let doc2 = doc1.clone()
      doc1.free()
      doc2.free()
    })

    it('should be able to start and commit', () => {
      let doc = Automerge.init()
      doc.begin()
      doc.commit()
    })

    it('calling begin inside a transaction should throw an error', () => {
      let doc = Automerge.init()
      doc.begin()
      assert.throws(() => { doc.begin() }, Error);
    })

    it('calling commit outside a transaction should throw an error', () => {
      let doc = Automerge.init()
      assert.throws(() => { doc.commit() }, Error);
    })

    it('getting a nonexistant prop does not throw an error', () => {
      let doc = Automerge.init()
      let root = Automerge.root()
      let result = doc.value(root,"hello")
      assert.deepEqual(result,[])
    })

    it('should be able to set and get a simple value', () => {
      let doc = Automerge.init()
      let root = Automerge.root()
      let result

      doc.begin()
      doc.set(root, "hello", "world")
      doc.set(root, "number", 5, "uint")
      doc.commit()

      result = doc.value(root,"hello")
      assert.deepEqual(result,["str","world"])

      result = doc.value(root,"number")
      assert.deepEqual(result,["uint",5])

      //doc.dump()
    })

    it('should be able to make sub objects', () => {
      let doc = Automerge.init()
      let root = Automerge.root()
      let result

      doc.begin()
      let submap = doc.makeMap(root, "submap")
      doc.commit()

      result = doc.value(root,"submap")
      assert.deepEqual(result,["map","world"])

      doc.dump()
    })
  })
})
