use std::env;

use duct::cmd;

// #[test]
// fn import_stdin() {
//     let bin = env!("CARGO_BIN_EXE_automerge");
//     let initial_state_json = serde_json::json!({
//         "birds": {
//             "wrens": 3.0,
//             "sparrows": 15.0
//         }
//     });
//     let json_bytes = serde_json::to_string_pretty(&initial_state_json).unwrap();

//     let no_pipe_no_file = cmd!(bin, "import").stdin_bytes(json_bytes.clone()).run();

//     assert!(no_pipe_no_file.is_err());

//     let pipe_no_file = cmd!(bin, "import")
//         .stdin_bytes(json_bytes.clone())
//         .stdout_capture()
//         .run();

//     assert!(pipe_no_file.is_ok());

//     let mut temp_file = std::env::temp_dir();
//     temp_file.push("import_test.mpl");
//     let no_pipe_file = cmd!(bin, "import", "--out", &temp_file)
//         .stdin_bytes(json_bytes)
//         .run();

//     assert!(no_pipe_file.is_ok());
//     std::fs::remove_file(temp_file).unwrap();
// }

// #[test]
// fn export_stdout() {
//     let bin = env!("CARGO_BIN_EXE_automerge");
//     let no_pipe_no_file = cmd!(bin, "export").stdout_capture().run();

//     assert!(no_pipe_no_file.is_err());
// }

#[test]
fn import_export_isomorphic() {
    let bin = env!("CARGO_BIN_EXE_automerge");
    let initial_state_json = serde_json::json!({
        "birds": {
            "wrens": 3.0,
            "sparrows": 15.0
        }
    });
    let json_bytes = serde_json::to_string_pretty(&initial_state_json).unwrap();

    let stdout = cmd!(bin, "import")
        .stdin_bytes(json_bytes.clone())
        .pipe(cmd!(bin, "export"))
        .read()
        .unwrap();
    assert_eq!(stdout, json_bytes);
}

/*
#[test]
fn import_change_export() {
    let bin = env!("CARGO_BIN_EXE_automerge");
    let initial_state_json = serde_json::json!({
        "birds": {
            "wrens": 3.0,
            "sparrows": 15.0
        }
    });
    let json_bytes = serde_json::to_string_pretty(&initial_state_json).unwrap();

    let stdout = cmd!(bin, "import")
        .stdin_bytes(json_bytes.clone())
        .pipe(cmd!(bin, "change", "set $[\"birds\"][\"owls\"] 12.0"))
        .stdin_bytes(json_bytes)
        .pipe(cmd!(bin, "export"))
        .read()
        .unwrap();
    let result: serde_json::Value = serde_json::from_str(stdout.as_str()).unwrap();
    let expected = serde_json::json!({
        "birds": {
            "wrens": 3.0,
            "sparrows": 15.0,
            "owls": 12.0,
        }
    });
    assert_eq!(result, expected);
}
*/

/// Build a document with two sequential changes and return its serialised bytes
/// together with the change hash created by each change.
fn two_change_doc() -> (Vec<u8>, automerge::ChangeHash, automerge::ChangeHash) {
    use automerge::{transaction::Transactable, AutoCommit, ROOT};

    let mut doc = AutoCommit::new();
    doc.put(ROOT, "a", 1).unwrap();
    let first = doc.commit().unwrap();
    doc.put(ROOT, "b", 2).unwrap();
    let second = doc.commit().unwrap();
    (doc.save(), first, second)
}

#[test]
fn fork_at_earlier_change_drops_later_state() {
    let bin = env!("CARGO_BIN_EXE_automerge");
    let (doc, first, _second) = two_change_doc();

    let stdout = cmd!(bin, "fork", first.to_string())
        .stdin_bytes(doc)
        .pipe(cmd!(bin, "export"))
        .read()
        .unwrap();

    let result: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    // Forking at the first change should only contain state from that change.
    assert_eq!(result, serde_json::json!({ "a": 1 }));
}

#[test]
fn fork_at_head_preserves_state() {
    let bin = env!("CARGO_BIN_EXE_automerge");
    let (doc, _first, second) = two_change_doc();

    let stdout = cmd!(bin, "fork", second.to_string())
        .stdin_bytes(doc)
        .pipe(cmd!(bin, "export"))
        .read()
        .unwrap();

    let result: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(result, serde_json::json!({ "a": 1, "b": 2 }));
}

#[test]
fn fork_with_unknown_hash_fails() {
    let bin = env!("CARGO_BIN_EXE_automerge");
    let (doc, _first, _second) = two_change_doc();

    // A syntactically valid hash that is not present in the document.
    let absent = "0".repeat(64);
    let output = cmd!(bin, "fork", absent)
        .stdin_bytes(doc)
        .stdout_capture()
        .unchecked()
        .run()
        .unwrap();

    // The fork fails and produces no document on stdout.
    assert!(output.stdout.is_empty());
}
