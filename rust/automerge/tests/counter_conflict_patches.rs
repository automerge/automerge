//! Counter deltas and conflict metadata must both survive a view transition.

use automerge::transaction::Transactable;
use automerge::{
    ActorId, Author, AutoCommit, Config, ObjType, Prop, ReadDoc, ScalarValue, TextEncoding, ROOT,
};

#[test]
fn masking_an_increment_also_reports_a_new_conflict_for_maps_and_lists() {
    for list in [false, true] {
        let alice = Author::from(vec![1]);
        let bob = Author::from(vec![2]);
        let mut doc = AutoCommit::from_config(
            Config::default()
                .author(Some(bob))
                .text_encoding(TextEncoding::UnicodeCodePoint),
        )
        .with_actor(ActorId::from([1]))
        .unwrap();
        let obj = if list {
            doc.put_object(ROOT, "list", ObjType::List).unwrap()
        } else {
            ROOT
        };
        if list {
            doc.insert(&obj, 0, 9).unwrap();
        }
        doc.commit();
        let mut peer = doc
            .fork_with_author(Some(alice.clone()))
            .unwrap()
            .with_actor(ActorId::from([0xa0]))
            .unwrap();
        let prop = if list {
            Prop::Seq(0)
        } else {
            Prop::Map("value".into())
        };
        doc.put(&obj, prop.clone(), 0).unwrap();
        doc.commit();
        peer.put(&obj, prop.clone(), ScalarValue::counter(0))
            .unwrap();
        peer.commit();
        let boundary = peer.get_heads();
        peer.merge(&mut doc).unwrap();
        peer.increment(&obj, prop.clone(), -1).unwrap();
        peer.commit();
        doc.merge(&mut peer).unwrap();

        assert_eq!(doc.get_all(&obj, prop.clone()).unwrap().len(), 1);
        let mut model = doc.hydrate(ROOT, None).unwrap();
        doc.update_diff_cursor();
        doc.mask_author(alice, &boundary);
        let values = doc.get_all(&obj, prop.clone()).unwrap();
        assert_eq!(values.len(), 2);
        let patches = doc.diff_incremental();
        model
            .apply_patches(TextEncoding::UnicodeCodePoint, patches.clone())
            .unwrap();
        assert_eq!(
            model,
            doc.hydrate(ROOT, None).unwrap(),
            "list={list}, patches={patches:?}"
        );

        // The inverse transition removes the conflict and reapplies the delta.
        // It must remain a replacement rather than leave stale conflict metadata.
        doc.reveal_author(&Author::from(vec![1]));
        assert_eq!(doc.get_all(&obj, prop).unwrap().len(), 1);
        model
            .apply_patches(TextEncoding::UnicodeCodePoint, doc.diff_incremental())
            .unwrap();
        assert_eq!(model, doc.hydrate(ROOT, None).unwrap());
    }
}
