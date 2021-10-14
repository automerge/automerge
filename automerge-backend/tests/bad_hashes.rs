use automerge_protocol as amp;

// Reproduces the problem encountered in https://github.com/automerge/automerge-rs/issues/240
#[test]
fn mismatched_head_repro_one() {
    let op_json = serde_json::json!({
        "ops": [
            {
                "action": "del",
                "obj": "1@1485eebc689d47efbf8b892e81653eb3",
                "elemId": "3164@0dcdf83d9594477199f80ccd25e87053",
                "multiOp": 1,
                "pred": [
                    "3164@0dcdf83d9594477199f80ccd25e87053"
                ]
            },
        ],
        "actor": "e63cf5ed1f0a4fb28b2c5bc6793b9272",
        "hash": "e7fd5c02c8fdd2cdc3071ce898a5839bf36229678af3b940f347da541d147ae2",
        "seq": 1,
        "startOp": 3179,
        "time": 1634146652,
        "message": null,
        "deps": [
            "2603cded00f91e525507fc9e030e77f9253b239d90264ee343753efa99e3fec1"
        ]
    });

    let change: amp::Change = serde_json::from_value(op_json).unwrap();
    let expected_hash: amp::ChangeHash =
        "4dff4665d658a28bb6dcace8764eb35fa8e48e0a255e70b6b8cbf8e8456e5c50"
            .parse()
            .unwrap();
    let encoded: automerge_backend::Change = change.into();
    assert_eq!(encoded.hash, expected_hash);
}
