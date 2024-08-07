import assert from 'assert'
import { create } from '../nodejs/automerge_wasm.cjs'

describe("the patches", () => {
    describe("when putting objects in a map", () => {
        it("should emit a patch with the full value", () => {
            const doc = create()
            const list = doc.putObject("/", "list", [])
            const map = doc.putObject("/", "map", {})
            const text = doc.putObject("/", "text", "")
            const patches = doc.diffIncremental()
            assert.deepStrictEqual(patches, [
                {
                    action: 'put',
                    path: [
                        'list'
                    ],
                    value: [],
                    taggedValue: { datatype: "list", value: list, opid: list },
                },
                {
                    action: 'put',
                    path: [
                        'map'
                    ],
                    value: {},
                    taggedValue: { datatype: "map", value: map, opid: map },
                },
                {
                    action: 'put',
                    path: [
                        'text'
                    ],
                    value: "",
                    taggedValue: { datatype: "text", value: text, opid: text },
                }
            ])
        })

    })

    describe("when putting objects in a list", () => {
        it("should emit a patch with the full value", () => {
            const doc = create()
            const outerList = doc.putObject("/", "outerList", [])
            doc.push(outerList, null)
            doc.push(outerList, null)
            doc.push(outerList, null)
            // Pop the patches so far as we don't care about them
            doc.diffIncremental()
            const list = doc.putObject("/outerList", 0, [])
            const map = doc.putObject("/outerList", 1, {})
            const text = doc.putObject("/outerList", 2, "")
            const patches = doc.diffIncremental()
            assert.deepStrictEqual(patches, [
                {
                    action: 'put',
                    path: [
                        'outerList',
                        0
                    ],
                    value: [],
                    taggedValue: { datatype: "list", value: list, opid: list },
                },
                {
                    action: 'put',
                    path: [
                        'outerList',
                        1,
                    ],
                    value: {},
                    taggedValue: { datatype: "map", value: map, opid: map },
                },
                {
                    action: 'put',
                    path: [
                        'outerList',
                        2
                    ],
                    value: "",
                    taggedValue: { datatype: "text", value: text, opid: text },
                }
            ])
        })
    })

    describe("when inserting objects in a list", () => {
        it("should emit a patch with the full value", () => {
            const doc = create()
            const outerList = doc.putObject("/", "outerList", [])
            // Pop the patches so far as we don't care about them
            doc.diffIncremental()
            const list = doc.insertObject("/outerList", 0, [])
            const map = doc.insertObject("/outerList", 1, {})
            const text = doc.insertObject("/outerList", 2, "")
            const patches = doc.diffIncremental()
            assert.deepStrictEqual(patches, [
                {
                    action: 'insert',
                    path: [
                        'outerList',
                        0
                    ],
                    values: [[], {}, ""],
                    taggedValues: [
                        { datatype: "list", value: list, opid: list },
                        { datatype: "map", value: map, opid: map },
                        { datatype: "text", value: text, opid: text },
                    ],
                },
            ])
        })
    })

})
