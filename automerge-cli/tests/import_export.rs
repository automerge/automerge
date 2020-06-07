use duct::cmd;
use std::env;

#[test]
fn import_stdin() {
    let bin = env!("CARGO_BIN_EXE_automerge");
    let initial_state_json: serde_json::Value = serde_json::from_str(
        r#"{
    "birds": {
        "wrens": 3.0,
        "sparrows": 15.0
    }
}"#,
    )
    .unwrap();
    let json_bytes = serde_json::to_string_pretty(&initial_state_json).unwrap();

    let no_pipe_no_file = cmd!(bin, "import").stdin_bytes(json_bytes.clone()).run();

    assert!(no_pipe_no_file.is_err());

    let pipe_no_file = cmd!(bin, "import")
        .stdin_bytes(json_bytes.clone())
        .stdout_capture()
        .run();

    assert!(pipe_no_file.is_ok());

    let mut temp_file = std::env::temp_dir();
    temp_file.push("import_test.mpl");
    let no_pipe_file = cmd!(bin, "import", "--out", &temp_file)
        .stdin_bytes(json_bytes.clone())
        .run();

    assert!(no_pipe_file.is_ok());
    std::fs::remove_file(temp_file.clone()).unwrap();
}

#[test]
fn export_stdout() {
    let bin = env!("CARGO_BIN_EXE_automerge");
    let no_pipe_no_file = cmd!(bin, "export").stdout_capture().run();

    assert!(no_pipe_no_file.is_err());
}

#[test]
fn import_export_isomorphic() {
    let bin = env!("CARGO_BIN_EXE_automerge");
    let initial_state_json: serde_json::Value = serde_json::from_str(
        r#"{
    "birds": {
        "wrens": 3.0,
        "sparrows": 15.0
    }
}"#,
    )
    .unwrap();
    let json_bytes = serde_json::to_string_pretty(&initial_state_json).unwrap();

    let stdout = cmd!(bin, "import")
        .stdin_bytes(json_bytes.clone())
        .pipe(cmd!(bin, "export"))
        .read()
        .unwrap();
    assert_eq!(stdout, json_bytes);
}
