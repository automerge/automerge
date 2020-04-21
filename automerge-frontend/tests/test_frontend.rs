use automerge_frontend::{Value, Frontend, MapType, SequenceType};

#[test]
fn test_init_with_state(){
    let initial_state_json: serde_json::Value = serde_json::from_str(r#"
        {
            "birds": {
                "wrens": 3.0,
                "magpies": 4.0
            },
            "alist": ["one", 2.0]
        }
    "#).unwrap();
    let value = Value::from_json(&initial_state_json);
    println!("Value: {:?}", value);
    let frontend = Frontend::new_with_initial_state(value).unwrap();
    let result_state = frontend.state().to_json();
    assert_eq!(initial_state_json, result_state);
}
