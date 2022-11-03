//import * as Automerge from "@automerge/automerge"
import * as Automerge from "@automerge/automerge"
import * as fs from "fs"

const start = new Date()
let state = Automerge.from({text: new Automerge.Text()})

type WithInsert = [number, number, string]
type WithoutInsert = [number, number] 
const edits: Array<WithInsert | WithoutInsert> = JSON.parse(fs.readFileSync("./edits.json", {encoding: "utf8"}))

state = Automerge.change(state, doc => {
    const start2 = new Date()
    for (let i = 0; i < edits.length; i++) {
        if (i % 1000 === 0) {
            const elapsed2 = (new Date() as any) - (start2 as any)
            console.log(`processed 1000 edits in ${elapsed2}`)
        }
        let edit = edits[i]
        let [start, del, values] = edit
        doc.text.deleteAt!(start, del)
        if (values != null) {
            doc.text.insertAt!(start, ...values)
        }
    }
})

let elapsed = (new Date() as any) - (start as any)
console.log(`Done in ${elapsed} ms`)

