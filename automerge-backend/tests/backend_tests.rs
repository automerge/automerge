extern crate automerge_backend;
use automerge_backend::{Change, Backend, Patch};

/// 
/// it('should assign to a key in a map', () => {
///   const actor = uuid()
///   const change1 = {actor, seq: 1, startOp: 1, time: 0, deps: {}, ops: [
///     {action: 'set', obj: ROOT_ID, key: 'bird', value: 'magpie', pred: []}
///   ]}
///   const s0 = Backend.init()
///   const [s1, patch1] = Backend.applyChanges(s0, [encodeChange(change1)])
///   assert.deepStrictEqual(patch1, {
///     version: 1, clock: {[actor]: 1}, canUndo: false, canRedo: false,
///     diffs: {objectId: ROOT_ID, type: 'map', props: {
///       bird: {[`1@${actor}`]: {value: 'magpie'}}
///     }}
///   })
/// })
///
#[test]
fn test_incremental_diffs_in_a_map(){
    let change: Change = serde_json::from_str(r#"{
        "actor": "7b7723af-d9e6-4803-97a4-d467b7693156", 
        "seq": 1, 
        "startOp": 1, 
        "time": 0, 
        "deps": {}, 
        "ops": [
            {
                "action": "set",
                "obj": "00000000-0000-0000-0000-000000000000",
                "key": "bird",
                "value": "magpie", 
                "pred": []
            }
        ]
    }"#).unwrap();
    let mut backend = Backend::init();
    let patch = backend.apply_changes(vec![change]).unwrap();
    let expected_patch: Patch = serde_json::from_str(r#"{
        "version": 1,
        "clock": {
            "7b7723af-d9e6-4803-97a4-d467b7693156": 1
        },
        "canUndo": false,
        "canRedo": false,
        "diffs": {
            "objectId": "00000000-0000-0000-0000-000000000000", 
            "type": "map", 
            "props": {
                "bird": {
                    "1@7b7723af-d9e6-4803-97a4-d467b7693156": {"value": "magpie"}
                }
            }
        }
    }"#).unwrap();
    assert_eq!(patch, expected_patch)
}
