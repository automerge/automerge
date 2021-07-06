use std::path::Path;

// Not really an example, more
// of a build script
use automerge_protocol as amp;
use serde::ser::Serialize;

fn main() {
    let file_path = Path::new(file!()).parent().unwrap();
    let cwd = std::env::current_dir().unwrap();
    let root = cwd.join(file_path).join("example-data/");
    let names = [
        "change1",
        "change2",
        "patch1",
        "patch2",
        "patch_small",
        "multi_element_insert",
    ];
    for name in &names {
        let json_name = root.join(format!("{}.json", name));
        let msgpack_name = root.join(format!("{}.mpk", name));

        let mut old_bytes = None;
        if let Ok(v) = std::fs::read(&msgpack_name) {
            old_bytes = Some(v);
        }

        let msgpack_name_copy = root.join(format!("{}.mpk.copy", name));
        let json = std::fs::read_to_string(&json_name)
            .unwrap_or_else(|_| panic!("Failed to read: {:?}", json_name));
        let mut buf = vec![];
        let mut serializer = rmp_serde::encode::Serializer::new(&mut buf)
            .with_struct_map()
            .with_string_variants();
        if name.contains("change") {
            let change: amp::Change = serde_json::from_str(&json).unwrap();
            change.serialize(&mut serializer).unwrap();
        } else if name.contains("multi_element_insert") {
            let multi: amp::DiffEdit = serde_json::from_str(&json).unwrap();
            multi.serialize(&mut serializer).unwrap();
        } else {
            let patch: amp::Patch = serde_json::from_str(&json).unwrap();
            // println!("{:?}", patch);
            patch.serialize(&mut serializer).unwrap();
        }
        // Write vec to file
        std::fs::write(&msgpack_name_copy, buf.clone()).unwrap();
        std::fs::rename(msgpack_name_copy, &msgpack_name).unwrap();

        // Check the generated mpack is valid
        let bytes = std::fs::read(&msgpack_name).unwrap();
        // ensure the write happened perfectly
        assert_eq!(bytes, buf);

        let mut rdr = std::io::Cursor::new(bytes.clone());
        println!("File: {:?}", msgpack_name);
        if let Err(e) = rmpv::decode::read_value(&mut rdr) {
            println!("Warning, failed to deserialize with error: {:?}", e);
        } else if let Some(old_bytes) = old_bytes {
            let mut diff = 0;
            for (old_byte, new_byte) in old_bytes.iter().zip(bytes.iter()) {
                if old_byte != new_byte {
                    diff += 1;
                }
            }
            if old_bytes == bytes {
                assert_eq!(diff, 0);
                println!("File is ok & did not get updated");
            } else {
                // This sometimes happens b/c JSON deserialization is not deterministic?
                // If you don't believe me -- uncomment the print statement when deserializing a
                // Patch. If you run the script multiple times w/o editing the JSON files, but the
                // message pack files change -- see if the output of the print statement differs
                // (for example, the order of the props in the patch deserialized from JSON might be different)
                println!("File content updated (& checks passed)! New len: {:?}, Old len: {:?}, n_diff: {:?}", old_bytes.len(), bytes.len(), diff);
            }
        } else {
            println!("File newly created!");
        }
        println!("===");
    }
}
