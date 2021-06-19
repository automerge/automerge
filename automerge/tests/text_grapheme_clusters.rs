use smol_str::SmolStr;
use unicode_segmentation::UnicodeSegmentation;

#[test]
fn create_frontend_with_grapheme_clusters() {
    let mut hm = std::collections::HashMap::new();
    hm.insert(
        SmolStr::default(),
        automerge::Value::Text("\u{80}".graphemes(true).map(|s| s.into()).collect()),
    );
    let (mut f, c) =
        automerge::Frontend::new_with_initial_state(automerge::Value::Map(hm)).unwrap();
    let mut b = automerge::Backend::new();
    let (p, _) = b.apply_local_change(c).unwrap();
    f.apply_patch(p).unwrap();
}
