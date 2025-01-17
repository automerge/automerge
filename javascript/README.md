
## Status

This is a preview of the next major release of automerge.  The primary goal has been to reduce the memory footprint of automerge by implementing columnar compression on in memory ops and changes and to do so with as little cost to performance as possible.  This is to get the code into the hands of early adopters and start to smoke out problems.  PLEASE do not use this with any important data.

## Known Issues

The code is not fully optimized but reading and writing to a document should be at worst 2x to 10x of the mainline release's performance.

These are the issues know of at the time of release.  Issues will be delbt with in order of criticality.

### Critical

The test `automerge-wasm::sync::should report whether the other end has our changes` fails about one time in 10.

### Hight

The opset is missing an index related to repeated updates on a single map key or list element.  Reading or writing to these elements triggers a sequential scan of all past updates leading to O(N) performance.
The internal get_object_type() method needs to be optimized leading to poor performance in documents with many objects or a large number of ops.

### Medium

Continue to increase performance to be on par with current mainline releases.
Implement columnar compress of the change graph.

## Api Changes

Currently the only external api change has been that methods that used to return references to changes chached interally (get_change_by_hash, get_changes, get_last_local_change) now return the owned changes.

## Benchmarks

All benchmarks were done on an apple M1 processor in native rust using (orionz/automerge-battery)[https://github.com/orionz/automerge-battery]

### Edit Trace

Reads and writes are not fully optimized and will be improved before release.

------------------------------------------------------------------+
|       test             |    op_set2     |    main    |  ratio   |
------------------------------------------------------------------+
| edit_trace_many_tx     |    10.4 s      |  965.1 ms  |  10.77   |
| edit_trace_single_tx   |     4.9 s      |    323 ms  |  15.17   |
------------------------------------------------------------------+

### Load And Save (median score, N=100,000)

Save and load is alraedy benefiting from the new memory footprint.

------------------------------------------------------------------+
|       document         |    op_set2     |    main    |  ratio   |
------------------------------------------------------------------+
| load_big_paste         |    22.33 ms    |  108.3 ms  |  0.206   |
| load_chunky            |    22.96 ms    |  122.1 ms  |  0.188   |
| load_typing            |    133.7 ms    |  345.5 ms  |  0.387   |
| save_big_paste         |     2.16 ms    |   11.7 ms  |  0.185   |
| save_chunky            |     2.97 ms    |  12.78 ms  |  0.232   |
| save_typing            |     5.35 ms    |  39.63 ms  |  0.135   |
------------------------------------------------------------------+

### Memory Usage

Memory usage is greatly improved.  The document `big_paste` is the best case scenario with a single transaction with a single 100,000 character paste.  The `chunky` document has 1,000 pastes with 100 characters each representing the middle ground.  And the `typing` document has 100,000 changes with a single op each.

```
------------------------------------------------------------------+
|       document         |    op_set2     |    main    |  ratio   |
------------------------------------------------------------------+
| big_paste              |    106.7 KB    |   40.3 MB  |  0.002   |
| chunky                 |    174.8 KB    |   38.2 MB  |  0.004   |
| typing                 |     19.4 MB    |    126 MB  |  0.153   |
------------------------------------------------------------------+
```

