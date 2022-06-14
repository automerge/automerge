
## Methods we need to support

### Basic management 

  1. `AMcreate()`
  1. `AMclone(doc)`
  1. `AMfree(doc)`
  1. `AMconfig(doc, key, val)` // set actor
  1. `actor = get_actor(doc)`

### Transactions

  1. `AMpendingOps(doc)`
  1. `AMcommit(doc, message, time)`
  1. `AMrollback(doc)`

### Write 

  1. `AMset{Map|List}(doc, obj, prop, value)`
  1. `AMinsert(doc, obj, index, value)`
  1. `AMpush(doc, obj, value)`
  1. `AMdel{Map|List}(doc, obj, prop)`
  1. `AMinc{Map|List}(doc, obj, prop, value)`
  1. `AMspliceText(doc, obj, start, num_del, text)`

### Read

  1. `AMkeys(doc, obj, heads)`
  1. `AMlength(doc, obj, heads)`
  1. `AMvalues(doc, obj, heads)`
  1. `AMtext(doc, obj, heads)`

### Sync

  1. `AMgenerateSyncMessage(doc, state)`
  1. `AMreceiveSyncMessage(doc, state, message)`
  1. `AMinitSyncState()`

### Save / Load

  1. `AMload(data)`
  1. `AMloadIncremental(doc, data)`
  1. `AMsave(doc)`
  1. `AMsaveIncremental(doc)`

### Low Level Access

  1. `AMapplyChanges(doc, changes)`
  1. `AMgetChanges(doc, deps)`
  1. `AMgetChangesAdded(doc1, doc2)`
  1. `AMgetHeads(doc)`
  1. `AMgetLastLocalChange(doc)`
  1. `AMgetMissingDeps(doc, heads)`

### Encode/Decode

  1. `AMencodeChange(change)`
  1. `AMdecodeChange(change)`
  1. `AMencodeSyncMessage(change)`
  1. `AMdecodeSyncMessage(change)`
  1. `AMencodeSyncState(change)`
  1. `AMdecodeSyncState(change)`

## Open Question - Memory management

Most of these calls return one or more items of arbitrary length.  Doing memory management in C is tricky.  This is my proposed solution...

### 

  ```
    // returns 1 or zero opids
    n = automerge_set(doc, "_root", "hello", datatype, value);
    if (n) {
      automerge_pop(doc, &obj, len);
    }

    // returns n values
    n = automerge_values(doc, "_root", "hello");
    for (i = 0; i<n ;i ++) {
      automerge_pop_value(doc, &value, &datatype, len);
    }
  ```

  There would be one pop method per object type.  Users allocs and frees the buffers.  Multiple return values would result in multiple pops. Too small buffers would error and allow retry.


### Formats

Actors - We could do (bytes,len) or a hex encoded string?.  
ObjIds - We could do flat bytes of the ExId struct but lets do human readable strings for now - the struct would be faster but opque
Heads - Might as well make it a flat buffer `(n, hash, hash, ...)`
Changes - Put them all in a flat concatenated buffer
Encode/Decode - to json strings?

