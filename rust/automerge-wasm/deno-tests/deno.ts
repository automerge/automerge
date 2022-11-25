// @deno-types="../index.d.ts"
import { create } from '../deno/automerge_wasm.js'

Deno.test("It should create, clone and free", () => {
  const doc1 = create()
  const doc2 = doc1.clone()
  doc2.free()
});
