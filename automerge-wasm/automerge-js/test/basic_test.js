
const assert = require('assert')
const util = require('util')
const Automerge = require('..')

describe('Automerge', () => {
    describe('basics', () => {
        it('should init clone and free', () => {
            let doc1 = Automerge.init()
            let doc2 = Automerge.clone(doc1);
        })

        it('handle basic set and read on root object', () => {
            let doc1 = Automerge.init()
            let doc2 = Automerge.change(doc1, (d) => {
              d.hello = "world"
              d.big = "little"
              d.zip = "zop"
              d.app = "dap"
            assert.deepEqual(d, {  hello: "world", big: "little", zip: "zop", app: "dap" })
            })
            assert.deepEqual(doc2, {  hello: "world", big: "little", zip: "zop", app: "dap" })
        })

        it('handle basic sets over many changes', () => {
            let doc1 = Automerge.init()
            let timestamp = new Date();
            let counter = new Automerge.Counter(100);
            let bytes = new Uint8Array([10,11,12]);
            let doc2 = Automerge.change(doc1, (d) => {
              d.hello = "world"
            })
            let doc3 = Automerge.change(doc2, (d) => {
              d.counter1 = counter
            })
            let doc4 = Automerge.change(doc3, (d) => {
              d.timestamp1 = timestamp
            })
            let doc5 = Automerge.change(doc4, (d) => {
              d.app = null
            })
            let doc6 = Automerge.change(doc5, (d) => {
              d.bytes1 = bytes
            })
            let doc7 = Automerge.change(doc6, (d) => {
              d.uint = new Automerge.Uint(1)
              d.int = new Automerge.Int(-1)
              d.float64 = new Automerge.Float64(5.5)
              d.number1 = 100
              d.number2 = -45.67
              d.true = true
              d.false = false
            })

            assert.deepEqual(doc7, {  hello: "world", true: true, false: false, int: -1, uint: 1, float64: 5.5, number1: 100, number2: -45.67, counter1: counter, timestamp1: timestamp, bytes1: bytes, app: null })

            let changes = Automerge.getAllChanges(doc7)
            let t1 = Automerge.init()
            ;let [t2] = Automerge.applyChanges(t1, changes)
            assert.deepEqual(doc7,t2)
        })

        it('handle overwrites to values', () => {
            let doc1 = Automerge.init()
            let doc2 = Automerge.change(doc1, (d) => {
              d.hello = "world1"
            })
            let doc3 = Automerge.change(doc2, (d) => {
              d.hello = "world2"
            })
            let doc4 = Automerge.change(doc3, (d) => {
              d.hello = "world3"
            })
            let doc5 = Automerge.change(doc4, (d) => {
              d.hello = "world4"
            })
            assert.deepEqual(doc5, {  hello: "world4" } )
        })

        it('handle set with object value', () => {
            let doc1 = Automerge.init()
            let doc2 = Automerge.change(doc1, (d) => {
              d.subobj = { hello: "world", subsubobj: { zip: "zop" } }
            })
            assert.deepEqual(doc2, { subobj:  { hello: "world", subsubobj: { zip: "zop" } } })
        })

        it('handle simple list creation', () => {
            let doc1 = Automerge.init()
            let doc2 = Automerge.change(doc1, (d) => d.list = [])
            assert.deepEqual(doc2, { list: []})
        })

        it('handle simple lists', () => {
            let doc1 = Automerge.init()
            let doc2 = Automerge.change(doc1, (d) => {
              d.list = [ 1, 2, 3 ]
            })
            assert.deepEqual(doc2.list.length, 3)
            assert.deepEqual(doc2.list[0], 1)
            assert.deepEqual(doc2.list[1], 2)
            assert.deepEqual(doc2.list[2], 3)
            assert.deepEqual(doc2, { list: [1,2,3] })
           // assert.deepStrictEqual(Automerge.toJS(doc2), { list: [1,2,3] })

            let doc3 = Automerge.change(doc2, (d) => {
              d.list[1] = "a"
            })

            assert.deepEqual(doc3.list.length, 3)
            assert.deepEqual(doc3.list[0], 1)
            assert.deepEqual(doc3.list[1], "a")
            assert.deepEqual(doc3.list[2], 3)
            assert.deepEqual(doc3, { list: [1,"a",3] })
        })
        it('handle simple lists', () => {
            let doc1 = Automerge.init()
            let doc2 = Automerge.change(doc1, (d) => {
              d.list = [ 1, 2, 3 ]
            })
            let changes = Automerge.getChanges(doc1, doc2)
            let docB1 = Automerge.init()
            ;let [docB2] = Automerge.applyChanges(docB1, changes)
            assert.deepEqual(docB2, doc2);
        })
        it('handle text', () => {
            let doc1 = Automerge.init()
            let tmp = new Automerge.Text("hello")
            let doc2 = Automerge.change(doc1, (d) => {
              d.list = new Automerge.Text("hello")
              d.list.insertAt(2,"Z")
            })
            let changes = Automerge.getChanges(doc1, doc2)
            let docB1 = Automerge.init()
            ;let [docB2] = Automerge.applyChanges(docB1, changes)
            assert.deepEqual(docB2, doc2);
        })

        it('have many list methods', () => {
            let doc1 = Automerge.from({ list: [1,2,3] })
            assert.deepEqual(doc1, { list: [1,2,3] });
            let doc2 = Automerge.change(doc1, (d) => {
              d.list.splice(1,1,9,10)
            })
            assert.deepEqual(doc2, { list: [1,9,10,3] });
            let doc3 = Automerge.change(doc2, (d) => {
              d.list.push(11,12)
            })
            assert.deepEqual(doc3, { list: [1,9,10,3,11,12] });
            let doc4 = Automerge.change(doc3, (d) => {
              d.list.unshift(2,2)
            })
            assert.deepEqual(doc4, { list: [2,2,1,9,10,3,11,12] });
            let doc5 = Automerge.change(doc4, (d) => {
              d.list.shift()
            })
            assert.deepEqual(doc5, { list: [2,1,9,10,3,11,12] });
            let doc6 = Automerge.change(doc5, (d) => {
              d.list.insertAt(3,100,101)
            })
            assert.deepEqual(doc6, { list: [2,1,9,100,101,10,3,11,12] });
        })
    })
})
