use automerge::{ActorId, Patch, PatchAction, Prop};
use proptest::prelude::Strategy;

mod caller_held;
mod internal_held;

/// A unique actor whose first byte controls its sort position. The trailing
/// counter guarantees uniqueness so that forked lineages never share an actor
/// (concurrent changes by the same actor are illegal in automerge).
pub fn actor(sort_byte: u8, uniq: &mut u32) -> ActorId {
    *uniq += 1;
    let mut bytes = vec![sort_byte];
    bytes.extend_from_slice(&uniq.to_be_bytes());
    ActorId::from(bytes.as_slice())
}

/// Generate random text for use within generated operations.
pub fn gen_text() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-z]{1,4}").unwrap()
}

/// Apply patches to the model string the way a materialized view would.
/// Only patches addressing ROOT."text" matter for the model; a PutMap of
/// "text" (re)creates the object, resetting the view of it.
pub fn apply_patches(model: &mut String, patches: &[Patch]) {
    for patch in patches {
        let at_text =
            patch.path.len() == 1 && matches!(&patch.path[0].1, Prop::Map(k) if k == "text");
        match &patch.action {
            PatchAction::PutMap { key, .. } if patch.path.is_empty() && key == "text" => {
                model.clear();
            }
            PatchAction::SpliceText { index, value, .. } if at_text => {
                let at = char_index(model, *index);
                model.insert_str(at, &value.make_string());
            }
            PatchAction::DeleteSeq { index, length } if at_text => {
                let start = char_index(model, *index);
                let end = char_index(model, index + length);
                model.replace_range(start..end, "");
            }
            _ => {}
        }
    }
}

/// Get the character index based on a provided `idx`, accounting for non-ASCII text.
fn char_index(s: &str, idx: usize) -> usize {
    s.char_indices().nth(idx).map(|(i, _)| i).unwrap_or(s.len())
}
