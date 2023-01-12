import * as Automerge from "../deno_dist/index.ts"

Deno.test("It should create, clone and free", () => {
  let doc1 = Automerge.init()
  let doc2 = Automerge.clone(doc1)

  // this is only needed if weakrefs are not supported
  Automerge.free(doc1)
  Automerge.free(doc2)
})
