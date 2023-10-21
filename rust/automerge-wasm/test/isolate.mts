
import { describe, it } from 'mocha';
//@ts-ignore
import assert from 'assert'
//@ts-ignore
import { load, create } from '../nodejs/automerge_wasm.cjs'

describe('Automerge', () => {
  describe('isolate', () => {
    it('it should be able to isolate', ()=> {
        // setup a simple text object
        let doc1 = create();
        doc1.putObject("/", "text", "aaabbbccc");
        assert.deepStrictEqual(doc1.text("/text"), "aaabbbccc");

        // record the init state
        let heads1 = doc1.getHeads();

        // make a change
        doc1.splice("/text", 3, 3, "BBB");
        assert.deepStrictEqual(doc1.text("/text"), "aaaBBBccc");

        // but then isolate to the orig state
        doc1.isolate(heads1)
        assert.deepStrictEqual(doc1.text("/text"), "aaabbbccc");

        // make a change in isolation
        doc1.splice("/text", 3, 3, "ZZZ");
        assert.deepStrictEqual(doc1.text("/text"), "aaaZZZccc");

        // fork off the doc and make changes
        let doc2 = doc1.fork();
        doc2.splice("/text",0,0,"000");
        assert.deepStrictEqual(doc2.text("/text"), "000aaaZZZBBBccc");

        // merging in outside changes will not show until you integrate
        doc1.merge(doc2)
        assert.deepStrictEqual(doc1.text("/text"), "aaaZZZccc");

        // yet more changes in isolation
        doc1.splice("/text", 7, 2, "CC");
        assert.deepStrictEqual(doc1.text("/text"), "aaaZZZcCC");

        doc1.integrate()
        /// Now we can see all the changes we couldnt before
        assert.deepStrictEqual(doc1.text("/text"), "000aaaZZZBBBcCC");
    })
  })
  describe('branches', () => {
    it('it should be able to create branches and check them out', ()=> {
        let doc1 = create();
        doc1.putObject("/", "text", "aaabbbccc");
        assert.deepStrictEqual(doc1.text("/text"), "aaabbbccc");
        assert.deepStrictEqual(doc1.currentBranch(), "main");
        assert.deepStrictEqual(doc1.branches(), ["main"]);

        let heads = doc1.getHeads();

        doc1.createBranch("branch2");

        assert.deepStrictEqual(doc1.text("/text"), "aaabbbccc");
        assert.deepStrictEqual(doc1.currentBranch(), "branch2");
        assert.deepStrictEqual(doc1.branches(), ["main", "branch2" ]);

        doc1.splice("/text", 3, 3, "BBB");
        assert.deepStrictEqual(doc1.text("/text"), "aaaBBBccc");

        doc1.createBranch("branch3", heads);
        assert.deepStrictEqual(doc1.text("/text"), "aaabbbccc");
        assert.deepStrictEqual(doc1.currentBranch(), "branch3");
        assert.deepStrictEqual(doc1.branches(), ["main", "branch2", "branch3" ]);

        doc1.splice("/text", 0, 3, "AAA");
        assert.deepStrictEqual(doc1.text("/text"), "AAAbbbccc");

        doc1.checkout("main");
        assert.deepStrictEqual(doc1.text("/text"), "aaabbbccc");

        doc1.checkout("branch2");
        assert.deepStrictEqual(doc1.text("/text"), "aaaBBBccc");

        doc1.checkout("main");
        doc1.mergeBranch("branch2");
        doc1.mergeBranch("branch3");

        assert.deepStrictEqual(doc1.currentBranch(), "main");
        assert.deepStrictEqual(doc1.branches(), ["main", "branch2", "branch3" ]);
        assert.deepStrictEqual(doc1.text("/text"), "AAABBBccc");

        let doc2 = load(doc1.save())

        assert.deepStrictEqual(doc2.currentBranch(), "main");
        assert.deepStrictEqual(doc2.branches(), ["main", "branch2", "branch3" ]);
        assert.deepStrictEqual(doc2.text("/text"), "AAABBBccc");
        doc2.checkout("branch2");
        assert.deepStrictEqual(doc2.text("/text"), "aaaBBBccc");

        let doc3 = create()
        doc3.applyChanges(doc2.getChanges([]))

        assert.deepStrictEqual(doc3.currentBranch(), "main");
        assert.deepStrictEqual(doc3.branches(), ["main", "branch2", "branch3" ]);
        assert.deepStrictEqual(doc3.text("/text"), "AAABBBccc");
        doc3.checkout("branch3");
        assert.deepStrictEqual(doc3.text("/text"), "AAAbbbccc");

    })
  })
})
