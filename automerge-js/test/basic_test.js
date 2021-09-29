
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
            })
            assert.deepEqual(Automerge.toJS(doc2), {  hello: "world", big: "little", zip: "zop", app: "dap" })
        })

        it('handle set with object value', () => {
            let doc1 = Automerge.init()
            let doc2 = Automerge.change(doc1, (d) => {
              d.subobj = { hello: "world", subsubobj: { zip: "zop" } }
            })
            assert.deepEqual(Automerge.toJS(doc2), { subobj:  { hello: "world", subsubobj: { zip: "zop" } } })
        })
    })
})
