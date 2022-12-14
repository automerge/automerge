import * as assert from 'assert'
import { RedisSync } from "../src"
import { Automerge, create } from "@automerge/automerge-wasm"

describe('redis-sync', () => {
    describe('basics', () => {
        it('should be able to connect and disconnect', async () => {
            let doc = create();
            let sync = new RedisSync({
                redis: "redis://",
                docId: "DOC124",
                clientId: "client01",
                init: (doc) => { 
                  console.log("DOC",doc);
                  doc.put("/","hello","world")
                }
            });
            await sync.connect();
            await new Promise(resolve => setTimeout(resolve, 1000))
            //await sync.disconnect();
        })
    })
})
