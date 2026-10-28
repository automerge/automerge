//! Historical sequence lookups agree with current-state lookups.
//!
//! Reads at historical heads (`parents_at`, `get_cursor_position(.., Some(heads))`)
//! go through the slow, clock-scoped sequence lookup, whereas reads at the
//! current heads use the indexed fast path. Two peers edit a shared list (of
//! map objects) and a shared text object, merging at random. For every heads
//! observed along the way, a historical read on the final document must equal
//! the same read on `fork_at(heads)`, which answers it from the fast path.
//! Objects and cursors that don't exist yet at those heads must not panic.

use automerge::{
    transaction::Transactable, ActorId, AutoCommit, ChangeHash, Cursor, MoveCursor, ObjId, ObjType,
    ReadDoc, TextEncoding, ROOT,
};
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum Op {
    InsertList {
        peer: usize,
        pos: usize,
    },
    DeleteList {
        peer: usize,
        pos: usize,
    },
    SpliceText {
        peer: usize,
        pos: usize,
        del: usize,
        text: String,
    },
    Merge,
}

fn gen_op() -> impl Strategy<Value = Op> {
    let peer = 0..2usize;
    prop_oneof![
        3 => (peer.clone(), any::<usize>()).prop_map(|(peer, pos)| Op::InsertList { peer, pos }),
        2 => (peer.clone(), any::<usize>()).prop_map(|(peer, pos)| Op::DeleteList { peer, pos }),
        3 => (peer, any::<usize>(), 0..3usize, "[aé😀]{0,3}")
            .prop_map(|(peer, pos, del, text)| Op::SpliceText { peer, pos, del, text }),
        1 => Just(Op::Merge),
    ]
}

struct Cursors {
    obj: ObjId,
    cursors: Vec<Cursor>,
}

impl Cursors {
    fn add(&mut self, doc: &AutoCommit, pos: usize) {
        for mv in [MoveCursor::After, MoveCursor::Before] {
            self.cursors
                .push(doc.get_cursor_moving(&self.obj, pos, None, mv).unwrap());
        }
    }
}

fn run(ops: Vec<Op>) -> Result<(), TestCaseError> {
    let mut a = AutoCommit::new_with_encoding(TextEncoding::UnicodeCodePoint)
        .with_actor(ActorId::from(b"aaaa"));
    let list = a.put_object(ROOT, "list", ObjType::List).unwrap();
    let text = a.put_object(ROOT, "text", ObjType::Text).unwrap();
    a.commit();
    let mut b = a.fork().with_actor(ActorId::from(b"bbbb"));

    let mut children = Vec::new();
    let mut list_cursors = Cursors {
        obj: list.clone(),
        cursors: Vec::new(),
    };
    let mut text_cursors = Cursors {
        obj: text.clone(),
        cursors: Vec::new(),
    };
    let mut all_heads: Vec<Vec<ChangeHash>> = vec![a.get_heads()];

    for op in ops {
        match op {
            Op::Merge => {
                a.merge(&mut b).unwrap();
                b.merge(&mut a).unwrap();
            }
            Op::InsertList { peer, pos } => {
                let doc = if peer == 0 { &mut a } else { &mut b };
                let pos = pos % (doc.length(&list) + 1);
                children.push(doc.insert_object(&list, pos, ObjType::Map).unwrap());
                list_cursors.add(doc, pos);
                doc.commit();
            }
            Op::DeleteList { peer, pos } => {
                let doc = if peer == 0 { &mut a } else { &mut b };
                let len = doc.length(&list);
                if len > 0 {
                    doc.delete(&list, pos % len).unwrap();
                    doc.commit();
                }
            }
            Op::SpliceText {
                peer,
                pos,
                del,
                text: s,
            } => {
                let doc = if peer == 0 { &mut a } else { &mut b };
                let len = doc.length(&text);
                let pos = pos % (len + 1);
                let del = del.min(len - pos);
                doc.splice_text(&text, pos, del as isize, &s).unwrap();
                for i in 0..s.chars().count() {
                    text_cursors.add(doc, pos + i);
                }
                doc.commit();
            }
        }
        all_heads.push(a.get_heads());
        all_heads.push(b.get_heads());
    }
    a.merge(&mut b).unwrap();
    all_heads.push(a.get_heads());

    for heads in &all_heads {
        let fork = a.fork_at(heads).unwrap();
        for child in &children {
            let historical = a.parents_at(child, heads).unwrap().collect::<Vec<_>>();
            match fork.parents(child) {
                Ok(current) => {
                    let current = current.collect::<Vec<_>>();
                    prop_assert_eq!(
                        &historical,
                        &current,
                        "parents of {:?} at {:?}",
                        child,
                        heads
                    );
                }
                Err(_) => {
                    // Not yet created at `heads`: the link into the list is hidden.
                    prop_assert_eq!(historical.len(), 2);
                    prop_assert!(!historical[0].visible);
                }
            }
        }
        for Cursors { obj, cursors } in [&list_cursors, &text_cursors] {
            for cursor in cursors {
                let historical = a.get_cursor_position(obj, cursor, Some(heads));
                // Cursors whose element doesn't exist yet have no fork to
                // compare against; computing `historical` must still not panic.
                if let Ok(current) = fork.get_cursor_position(obj, cursor, None) {
                    prop_assert_eq!(
                        historical,
                        Ok(current),
                        "cursor {:?} in {:?} at {:?}",
                        cursor,
                        obj,
                        heads
                    );
                }
            }
        }
    }
    Ok(())
}

proptest! {
    #[test]
    fn historical_seeks_match_fork(ops in proptest::collection::vec(gen_op(), 0..30)) {
        run(ops)?;
    }
}
