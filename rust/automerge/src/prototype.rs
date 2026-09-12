//! Disposable native-control experiment. No public API or format promise.
#![allow(dead_code)]
mod core;
#[cfg(test)]
mod format_observer;
mod policy;
#[cfg(test)]
mod structural_tests;
#[cfg(test)]
mod tests;
pub(crate) use core::decode_control;
#[cfg(test)]
use core::{record_control, Authority, Eligibility, Input, Session};
#[cfg(test)]
mod first_test {
    use super::{Authority, Eligibility, Input, Session};
    use crate::{transaction::Transactable, ActorId, Automerge, ReadDoc, ROOT};

    #[test]
    fn ex01_authority_checkpoints() {
        let mut doc = Automerge::new().with_actor(ActorId::from(vec![10]));
        let mut tx = doc.transaction();
        tx.put(ROOT, "title", "Trip plan").unwrap();
        let h = tx.commit().0.unwrap();
        doc.set_actor(ActorId::from(vec![20]));
        let mut tx = doc.transaction();
        tx.put(ROOT, "suggestion", "Camping").unwrap();
        let a = tx.commit().0.unwrap();
        let original = doc.get(ROOT, "suggestion").unwrap().unwrap().1;
        let r =
            super::record_control(&mut doc, ActorId::from(vec![30]), b"alice", vec![h]).unwrap();
        let mut tx = doc.transaction();
        tx.put(ROOT, "title", "Weekend plan").unwrap();
        let c = tx.commit().0.unwrap();
        assert_eq!(doc.get_change_by_hash(&c).unwrap().deps(), &[r]);
        let mut session = Session::new();
        session
            .deliver(
                doc.get_changes(&[])
                    .into_iter()
                    .map(|change| {
                        // Explicit trusted mock provenance, not inferred from receipt time.
                        let author = if change.actor_id() == &ActorId::from(vec![20]) {
                            b"alice".to_vec()
                        } else {
                            b"carol".to_vec()
                        };
                        Input::Content(change, author)
                    })
                    .collect(),
            )
            .unwrap();
        let pending = session.capture();
        assert_eq!(pending.eligibility[&a], Eligibility::Eligible);
        assert_eq!(pending.authority[&r], Authority::Pending);
        assert_eq!(
            session.scalar(&pending, "title"),
            Some("Weekend plan".into())
        );
        assert_eq!(
            session.scalar(&pending, "suggestion"),
            Some("Camping".into())
        );
        let t = session.deliver(vec![Input::Authorize(r)]).unwrap();
        let active = session.capture();
        assert_eq!(active.eligibility[&a], Eligibility::Excluded);
        assert_eq!(session.scalar(&active, "suggestion"), None);
        assert!(!t.status.is_empty());
        session.deliver(vec![Input::Invalidate(r)]).unwrap();
        let restored = session.capture();
        assert_eq!(restored.eligibility[&a], Eligibility::Eligible);
        assert_eq!(restored.authority[&r], Authority::Invalidated);
        assert_eq!(
            session.candidates(&restored, &ROOT, "suggestion")[0].1,
            original
        );
        for capture in [&pending, &active, &restored] {
            assert_eq!(capture.eligibility[&c], Eligibility::Eligible);
            assert_eq!(
                session.scalar(capture, "title"),
                Some("Weekend plan".into())
            );
        }
        assert_eq!(session.scalar(&active, "suggestion"), None);
    }
}
