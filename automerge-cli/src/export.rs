use anyhow::Result;

fn get_state_json(input_data: Vec<u8>) -> Result<serde_json::Value> {
    let mut backend = automerge_backend::Backend::new();
    let changes = automerge_backend::Change::load_document(&input_data)?;
    let patch = backend.apply_changes(changes)?;

    let mut frontend = automerge_frontend::Frontend::default();
    frontend.apply_patch(patch)?;

    Ok(frontend.state().to_json())
}

pub fn export_json(
    mut changes_reader: impl std::io::Read,
    mut writer: impl std::io::Write,
    is_tty: bool,
) -> Result<()> {
    let mut input_data = vec![];
    changes_reader.read_to_end(&mut input_data)?;

    let state_json = get_state_json(input_data)?;
    if is_tty {
        colored_json::write_colored_json(&state_json, &mut writer).unwrap()
    } else {
        writeln!(
            writer,
            "{}",
            serde_json::to_string_pretty(&state_json).unwrap()
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use automerge_frontend::Options;

    use super::*;

    #[test]
    fn cli_export_with_empty_input() {
        assert_eq!(get_state_json(vec![]).unwrap(), serde_json::json!({}))
    }

    #[test]
    fn cli_export_with_flat_map() {
        let initial_state_json: serde_json::Value =
            serde_json::from_str(r#"{"sparrows": 15.0}"#).unwrap();
        let value: automerge_frontend::Value =
            automerge_frontend::Value::from_json(&initial_state_json);

        let (_, initial_change) =
            automerge_frontend::Frontend::new_with_initial_state(value, Options::default())
                .unwrap();
        let mut backend = automerge_backend::Backend::new();
        backend.apply_local_change(initial_change).unwrap();

        let change_bytes = backend.save().unwrap();
        assert_eq!(
            get_state_json(change_bytes).unwrap(),
            serde_json::json!({"sparrows": 15.0})
        )
    }

    #[test]
    fn cli_export_with_nested_map() {
        let initial_state_json: serde_json::Value = serde_json::from_str(
            r#"{
    "birds": {
        "wrens": 3.0,
        "sparrows": 15.0
    }
}"#,
        )
        .unwrap();
        let value: automerge_frontend::Value =
            automerge_frontend::Value::from_json(&initial_state_json);

        let (_, initial_change) =
            automerge_frontend::Frontend::new_with_initial_state(value, Options::default())
                .unwrap();
        let mut backend = automerge_backend::Backend::new();
        backend.apply_local_change(initial_change).unwrap();

        let change_bytes = backend.save().unwrap();
        assert_eq!(
            get_state_json(change_bytes).unwrap(),
            serde_json::json!({
                "birds": {
                    "wrens": 3.0,
                    "sparrows": 15.0
                }
            })
        )
    }
}
