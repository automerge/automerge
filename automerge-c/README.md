
## Methods we need to support

### Basic management 

  1. `doc = create()`
  1. `doc = clone(doc)`
  1. `free(doc)`
  1. `set_actor(doc, actor)`
  1. `actor = get_actor(doc)`

### Transactions

  1. `pending_ops(doc)`
  1. `commit(doc, message, time)`
  1. `rollback(doc)`

### Write 

  1. `set(doc, obj, prop, value)`
  1. `insert(doc, obj, index, value)`
  1. `push(doc, obj, value)`
  1. `del(doc, obj, prop)`
  1. `inc(doc, obj, prop, value)`
  1. `splice_text(doc, obj, start, num_del, text)`

### Read

  1. `keys(doc, obj)`
  1. `keys_at(doc, obj, heads)`
  1. `length(doc, obj)`
  1. `length_at(doc, obj, heads)`
  1. `// value(doc, obj)`
  1. `// value_at(doc, obj, heads)`
  1. `values(doc, obj)`
  1. `values_at(doc, obj, heads)`
  1. `text(doc, obj)`
  1. `text_at(doc, obj, heads)`

### Sync

  1. `message = generate_sync_message(doc, state)`
  1. `receive_sync_message(doc, state, message)`
  1. `state = init_sync_state()`

### Save / Load

  1. `data = save(doc)`
  1. `doc = load(data)`
  1. `data = save_incremental(doc)`
  1. `load_incremental(doc, data)`

### Low Level Access

  1. `apply_changes(doc, changes)`
  1. `changes = get_changes(doc, deps)`
  1. `changes = get_changes_added(doc1, doc2)`
  1. `heads = get_heads(doc)`
  1. `change = get_last_local_change(doc)`
  1. `deps = get_missing_deps(doc, heads)`

### Encode/Decode

  1. `encode_change(change)`
  1. `decode_change(change)`
  1. `encode_sync_message(change)`
  1. `decode_sync_message(change)`
  1. `encode_sync_state(change)`
  1. `decode_sync_state(change)`

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

