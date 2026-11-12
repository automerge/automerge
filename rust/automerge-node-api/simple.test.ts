import assert from "node:assert/strict";
import { describe, it } from "node:test";

import * as lib from "@automerge/automerge-node-api";

describe("Automerge", () => {
  it("can load", () => {
    const Automerge = lib.create();
  });

  it("throws on invalid actor", () => {
    // TODO: The WebAssembly binding throws a RangeError
    const error = new Error(
      "could not parse Actor ID as a hex string: Odd number of digits"
    );
    assert.throws(() => {
      lib.create({ actor: "invalid-actor" });
    }, error);
  });

  it("can put", () => {
    const doc = lib.create();
    doc.put("/", "prop1", 100);
    assert.strictEqual(doc.get("/", "prop1"), 100);
  });
});
