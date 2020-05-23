use anyhow::Result;
use std::fs::read;
use std::path::Path;

fn get_state_json(input_data: Vec<u8>) -> Result<serde_json::Value> {
    let mut backend = automerge_backend::Backend::init();
    let patch = backend.apply_changes_binary(vec![input_data])?;

    let mut frontend = automerge_frontend::Frontend::new();
    frontend.apply_patch(patch).unwrap();

    Ok(frontend.state().to_json())
}

pub fn export_json(changes_file: &Path, mut writer: impl std::io::Write) -> Result<()> {
    let input_data = read(changes_file)?;

    let state_json = get_state_json(input_data)?;
    writeln!(
        writer,
        "{}",
        serde_json::to_string_pretty(&state_json).unwrap()
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_export_with_empty_input() {
        assert_eq!(get_state_json(vec![]).unwrap(), serde_json::json!({}))
    }

    #[test]
    fn cli_export_with_input() {
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
            automerge_frontend::Frontend::new_with_initial_state(value).unwrap();
        println!("{:?}", initial_change);
        let mut backend = automerge_backend::Backend::init();
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
