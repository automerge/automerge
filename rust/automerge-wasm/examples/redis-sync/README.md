
## Quick Example of Syncing Automerge Docs via Redis 

The good:
  1. store the document in redis as both a compacted document save and as individual changes
  2. able to load the the saved document, and then calculate which changes are missing in a single pass
  3. uses redis (RPUSH) to get global change ordering 
  4. wake all clients on new changes with redis (PUBLISH, SUBSCRIBE)
  5. elect (via SETNX) a single leader to saveIncremental() every 10 seconds
  6. said node also does a full doc save when incremental bytes are 10x the base save
  7. detects disconnection of the leader (via EXPIRE) and elects a new leader
  8. detects when a document has not been created and elects a single node to create it

The bad:
  1. redis can do binary data - i couldnt figure it out - i hex encode/decode everything
  2. there's probably a few race conditions i need to find
  3. my typescript is ass
  4. not much in the way of error handling
  5. should really be using applyPatches() but im not
  6. the api is a little funny 

How to use:

```
$ yarn
$ yarn build
```

Then run 

```
$ node client.js $DOCID
```

In two or three different windows and watch each client editing the same document.

