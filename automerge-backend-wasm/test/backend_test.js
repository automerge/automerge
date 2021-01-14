const assert = require('assert')
const Backend = require('..')

describe('Automerge.Backend', () => {
  describe('incremental diffs', () => {
    it('should assign to a key in a map', () => {
      const doc1 = Backend.init()
      const change = {
          actor: '55f250d0f76b4e15923600f98ebed8d7',
            seq: 1,
            startOp: 1,
            deps: [],
            time: 1609190674,
            message: '',
            ops: [
                  {
                          action: 'makeText',
                          obj: '_root',
                          key: 'text',
                          insert: false,
                          pred: []
                        },
                  {
                          action: 'set',
                          obj: '1@55f250d0f76b4e15923600f98ebed8d7',
                          key: '_head',
                          insert: true,
                          pred: [],
                          value: 'a'
                        },
                  {
                          action: 'makeMap',
                          obj: '1@55f250d0f76b4e15923600f98ebed8d7',
                          key: '2@55f250d0f76b4e15923600f98ebed8d7',
                          insert: true,
                          pred: []
                        },
                  {
                          action: 'set',
                          obj: '3@55f250d0f76b4e15923600f98ebed8d7',
                          key: 'attribute',
                          insert: false,
                          pred: [],
                          value: 'bold'
                        },
                ],
          extra_bytes: []
      }
      const doc2 = Backend.applyLocalChange(doc1, change)
    })
  })
})
