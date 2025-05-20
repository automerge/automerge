use anyhow::Result;
use automerge as am;

use crate::{color_json::print_colored_json, VerifyFlag};

fn get_state_json(input_data: Vec<u8>, skip: VerifyFlag) -> Result<serde_json::Value> {
    let doc = skip.load(&input_data).unwrap(); // FIXME
    serde_json::to_value(am::AutoSerde::from(&doc)).map_err(Into::into)
}

pub(crate) fn export_json(
    mut changes_reader: impl std::io::Read,
    mut writer: impl std::io::Write,
    skip: VerifyFlag,
    is_tty: bool,
) -> Result<()> {
    let mut input_data = vec![];
    changes_reader.read_to_end(&mut input_data)?;

    let state_json = get_state_json(input_data, skip)?;
    if is_tty {
        print_colored_json(&state_json).unwrap();
        writeln!(writer).unwrap();
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
    use super::*;
    use crate::import::initialize_from_json;

    #[test]
    fn cli_export_with_empty_input() {
        assert_eq!(
            get_state_json(vec![], Default::default()).unwrap(),
            serde_json::json!({})
        )
    }

    #[test]
    fn cli_export_with_flat_map() {
        let initial_state_json: serde_json::Value =
            serde_json::from_str(r#"{"sparrows": 15.0}"#).unwrap();
        //let value: am::Value = am::Value::from_json(&initial_state_json);
        //let (_, initial_change) = am::Frontend::new_with_initial_state(value).unwrap();
        //let mut backend = am::Automerge::new();
        //backend.apply_local_change(initial_change).unwrap();
        let mut backend = initialize_from_json(&initial_state_json).unwrap();
        let change_bytes = backend.save();
        assert_eq!(
            get_state_json(change_bytes, Default::default()).unwrap(),
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
        let mut backend = initialize_from_json(&initial_state_json).unwrap();
        /*
                let value: am::Value = am::Value::from_json(&initial_state_json);

                //let (_, initial_change) = am::Frontend::new_with_initial_state(value).unwrap();
                let mut backend = am::Automerge::new();
                //backend.apply_local_change(initial_change).unwrap();

        */
        let change_bytes = backend.save();
        assert_eq!(
            get_state_json(change_bytes, Default::default()).unwrap(),
            serde_json::json!({
                "birds": {
                    "wrens": 3.0,
                    "sparrows": 15.0
                }
            })
        )
    }
}
