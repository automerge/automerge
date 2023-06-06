/* eslint-disable no-undef */
import * as Automerge from "@automerge/automerge-wasm";

export default {
  async fetch() {
    const doc = Automerge.create();
    console.log("doc", doc);
    const edits = doc.putObject("_root", "edits", "");
    doc.splice(edits, 0, 0, "the quick fox jumps over the lazy dog");
    const doc2 = Automerge.load(doc.save());
    console.log("LOAD", Automerge.load);
    console.log("DOC", doc.materialize("/"));
    console.log("DOC2", doc2.materialize("/"));
    return Response.json(doc.text(edits));
  },
};
