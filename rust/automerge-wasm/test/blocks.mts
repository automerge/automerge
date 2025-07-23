import { describe, it } from "mocha";
import assert from "assert";
import { create, Automerge, ObjID } from "../nodejs/automerge_wasm.cjs";

describe("blocks", () => {
  describe("when splitting a block", () => {
    it("can split a block", () => {
      const doc = create({ actor: "aabbcc" });
      const text = doc.putObject("_root", "list", "🐻🐻🐻bbbccc");
      doc.splitBlock(text, 6, {
        type: "li",
        parents: ["ul"],
        attrs: { kind: "todo" },
      });
      const spans = doc.spans("/list");
      console.log(JSON.stringify(spans));
      assert.deepStrictEqual(spans, [
        { type: "text", value: "🐻🐻🐻" },
        {
          type: "block",
          value: { type: "li", parents: ["ul"], attrs: { kind: "todo" } },
        },
        { type: "text", value: "bbbccc" },
      ]);
    });

    it.skip("consolidates patches", () => {
      const doc = create({ actor: "aabbcc" });
      const list = doc.putObject("_root", "list", "🐻🐻🐻bbbccc");
      doc.updateDiffCursor();
      const doc2 = doc.fork();
      doc.splitBlock(list, 6, { type: "li", parents: ["ul"], attrs: {} });
      doc.joinBlock(list, 6);
      assert.deepStrictEqual(doc.text(list), "🐻🐻🐻bbbccc");
      assert.deepStrictEqual(doc.length(list), 12);

      doc2.updateDiffCursor();
      doc2.merge(doc);
      const patches3 = doc2.diffIncremental();
      assert.deepStrictEqual(patches3, []);
    });
  });

  describe("when joining a block", () => {
    let doc: Automerge;
    let text: ObjID;
    const block = { type: "unordered-list-item", parents: [], attrs: {} };

    beforeEach(() => {
      doc = create({ actor: "aabbcc" });
      text = doc.putObject("_root", "text", "aaabbbccc");
      doc.splitBlock(text, 3, block);
      doc.updateDiffCursor();
    });

    it("can join a block", () => {
      doc.joinBlock(text, 3);
      const spans = doc.spans("/text");
      assert.deepStrictEqual(spans, [{ type: "text", value: "aaabbbccc" }]);
    });
  });

  it.skip("patches correctly reference blocks", () => {
    const doc = create({ actor: "aabbcc" });
    const text = doc.putObject("_root", "text", "aaabbbccc");
    const starterHeads = doc.getHeads();
    doc.updateDiffCursor();
    const doc2 = doc.fork();
    const block = { type: "li", parents: ["ul"], attrs: {} };
    doc.splitBlock(text, 3, block);
    const blockCursor = doc.getCursor("/text", 3);
    //doc.updateBlock(text, blockId, "div", ["block","pre"]);
    const blockHeads = doc.getHeads();
    doc.joinBlock(text, 3);
    doc.commit();
    //let patches = doc.diffIncremental();
    //assert.deepStrictEqual(patches,[]);
    assert.deepStrictEqual(doc.text(text), "aaabbbccc");
    assert.deepStrictEqual(doc.length(text), 9);

    doc2.updateDiffCursor();
    doc2.merge(doc);
    //let patches2 = doc2.diffIncremental();
    //assert.deepStrictEqual(patches2,[]); // insert and delete
    const doc3 = doc.fork(undefined, blockHeads);
    const patches3A = doc3.diff([], doc3.getHeads());
    const patches3B = doc.diff([], blockHeads);
    const patches3C = doc.diff(blockHeads, starterHeads);
    assert.deepStrictEqual(patches3A, [
      { action: "put", path: ["text"], value: "" },
      { action: "splice", path: ["text", 0], value: "aaa" },
      { action: "insert", path: ["text", 3], values: [{}] },
      {
        action: "splitBlock",
        path: ["text", 4],
        index: 4,
        value: "bbbccc",
        block,
        cursor: blockCursor,
      },
    ]);
    assert.deepStrictEqual(patches3A, patches3B);
    assert.deepStrictEqual(patches3C, [
      { action: "joinBlock", index: 3, path: ["text", 3] },
    ]);
    // now make sure the patches look good on merge
    const doc4 = doc.fork(undefined, blockHeads);
    const doc5 = create();
    doc5.put("/", "a", "b");
    doc5.updateDiffCursor();
    doc5.merge(doc4);
    const patches3D = doc5.diffIncremental();
    assert.deepStrictEqual(patches3D, [
      { action: "put", path: ["text"], value: "" },
      { action: "splice", path: ["text", 0], value: "aaabbbccc" },
      {
        action: "splitBlock",
        path: ["text", 3],
        index: 3,
        block,
        cursor: blockCursor,
      },
    ]);
    const spans = doc5.spans("/text");
    assert.deepStrictEqual(spans, [
      { type: "text", value: "aaa" },
      { type: "block", value: block },
      { type: "text", value: "bbbccc" },
    ]);
  });

  it("references blocks on local changes", () => {
    const doc = create({ actor: "aabbcc" });
    const text = doc.putObject("_root", "text", "aaabbbccc");
    const block = { type: "li", parents: ["ul"], attrs: {} };
    doc.splitBlock(text, 3, block);
    doc.updateDiffCursor();
    doc.splice("/text", 6, 0, "AAA");
    const patches = doc.diffIncremental();
    assert.deepStrictEqual(patches, [
      { action: "splice", path: ["text", 6], value: "AAA" },
    ]);
    const spans = doc.spans("/text");
    assert.deepStrictEqual(spans, [
      { type: "text", value: "aaa" },
      { type: "block", value: block },
      { type: "text", value: "bbAAAbccc" },
    ]);
    assert.deepStrictEqual(doc.objInfo("/"), {
      id: "_root",
      type: "map",
      path: [],
    });
    assert.deepStrictEqual(doc.objInfo("/text"), {
      id: text,
      type: "text",
      path: ["text"],
    });
  });

  describe("when updating a block", () => {
    let doc: Automerge;
    let text: ObjID;
    const block = { type: "unordered-list-item", parents: [], attrs: {} };

    beforeEach(() => {
      doc = create({ actor: "aabbcc" });
      text = doc.putObject("_root", "text", "aaabbbccc");
      doc.splitBlock(text, 3, block);
      doc.updateDiffCursor();
    });

    describe("when updating a block type", () => {
      it("can update a block type", () => {
        doc.updateBlock(text, 3, {
          type: "ordered-list-item",
          parents: [],
          attrs: {},
        });
        const spans = doc.spans("/text");
        assert.deepStrictEqual(spans, [
          { type: "text", value: "aaa" },
          {
            type: "block",
            value: { type: "ordered-list-item", parents: [], attrs: {} },
          },
          { type: "text", value: "bbbccc" },
        ]);
      });
    });
  });

  describe("when updating all blocks via a diff", () => {
    it("can update multiple spans", () => {
      const doc = create();
      doc.putObject("_root", "text", "");
      doc.splitBlock("/text", 0, {
        type: "ordered-list-item",
        parents: [],
        attrs: { kind: "todo" },
      });
      doc.splice("/text", 1, 0, "first thing");
      doc.splitBlock("/text", 12, {
        type: "ordered-list-item",
        parents: [],
        attrs: { kind: "todo" },
      });
      doc.splice("/text", 13, 0, "second thing");
      doc.updateSpans(
        "/text",
        [
          {
            type: "block",
            value: {
              type: "paragraph",
              parents: [],
              attrs: { kind: "reallytodo" },
            },
          },
          { type: "text", value: "the first thing" },
          {
            type: "block",
            value: {
              type: "unordered-list-item",
              parents: ["ordered-list-item"],
              attrs: {},
            },
          },
          { type: "text", value: "the second thing" },
        ],
        null,
      );
      const spansAfter = doc.spans("/text");
      assert.deepStrictEqual(spansAfter, [
        {
          type: "block",
          value: {
            type: "paragraph",
            parents: [],
            attrs: { kind: "reallytodo" },
          },
        },
        { type: "text", value: "the first thing" },
        {
          type: "block",
          value: {
            type: "unordered-list-item",
            parents: ["ordered-list-item"],
            attrs: {},
          },
        },
        { type: "text", value: "the second thing" },
      ]);
    });

    it("can set external data types as block attributes", () => {
      const doc = create();
      class RawString {
        constructor(public value: string) {}
      }
      doc.registerDatatype(
        "str",
        (s: any) => new RawString(s),
        (s) => {
          if (s instanceof RawString) {
            return s.value;
          }
        },
      );
      doc.putObject("_root", "text", "hello world");
      doc.updateSpans(
        "/text",
        [
          // eslint-disable-next-line @typescript-eslint/ban-ts-comment
          // @ts-ignore
          {
            type: "block",
            value: { type: new RawString("paragraph"), parents: [], attrs: {} },
          },
          { type: "text", value: "hello world" },
        ],
        null,
      );
      const spansAfter = doc.spans("/text");
      assert.deepStrictEqual(spansAfter, [
        {
          type: "block",
          value: { type: new RawString("paragraph"), parents: [], attrs: {} },
        },
        { type: "text", value: "hello world" },
      ]);
    });

    describe("when updating block attributes with external data types", () => {
      let doc: Automerge;
      class RawString {
        constructor(public value: string) {}
      }

      beforeEach(() => {
        doc = create();
        doc.registerDatatype(
          "str",
          (s: any) => new RawString(s),
          (s) => {
            if (s instanceof RawString) {
              return s.value;
            }
          },
        );
        doc.putObject("_root", "text", "hello world");
        doc.splitBlock("/text", 0, {
          type: new RawString("paragraph"),
          parents: [new RawString("parent")],
          attrs: {},
        });
        doc.updateDiffCursor();
      });

      it("emits external datatype values in insert patches", () => {
        // Only change here is adding the "someparent" parent
        doc.updateSpans(
          "/text",
          [
            {
              type: "block",
              value: {
                type: new RawString("paragraph"),
                parents: [
                  new RawString("parent"),
                  new RawString("someotherparent"),
                ],
                attrs: {},
              },
            },
            { type: "text", value: "hello world" },
          ],
          null,
        );

        const patches = doc.diffIncremental();
        assert.deepStrictEqual(patches, [
          {
            action: "insert",
            path: ["text", 0, "parents", 1],
            values: [new RawString("someotherparent")],
          },
        ]);
      });

      it("emits external datatype values in put in map patches", () => {
        // Only change here is adding the "flavor" attribute
        doc.updateSpans(
          "/text",
          [
            {
              type: "block",
              value: {
                type: new RawString("paragraph"),
                parents: [new RawString("parent")],
                attrs: {},
                flavor: new RawString("chocolate"),
              },
            },
            { type: "text", value: "hello world" },
          ],
          null,
        );

        const patches = doc.diffIncremental();
        assert.deepStrictEqual(patches, [
          {
            action: "put",
            path: ["text", 0, "flavor"],
            value: new RawString("chocolate"),
          },
        ]);
      });

      it("emits external datatype values in put in seq patches", () => {
        // Only change here is adding the "flavor" attribute
        doc.updateSpans(
          "/text",
          [
            {
              type: "block",
              value: {
                type: new RawString("paragraph"),
                parents: [new RawString("grandparent")],
                attrs: {},
              },
            },
            { type: "text", value: "hello world" },
          ],
          null,
        );

        const patches = doc.diffIncremental();
        assert.deepStrictEqual(patches, [
          {
            action: "put",
            path: ["text", 0, "parents", 0],
            value: new RawString("grandparent"),
          },
        ]);
      });
    });
  });
});
