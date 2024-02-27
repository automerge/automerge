use automerge::{
    transaction::Transactable, Block, BlockOrText, NewBlock, ObjType, PatchAction, ReadDoc, ROOT,
};

#[test]
fn split_block_diff_incremental() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    doc.update_diff_cursor();
    let _block = doc
        .split_block(
            &text,
            5,
            NewBlock::new("ordered-list-item")
                .with_parents(["unordered-list-item", "ordered-list-item"].into_iter()),
        )
        .unwrap();
    let patches = doc.diff_incremental();
    assert_eq!(patches.len(), 1);
    assert_eq!(
        patches[0].action,
        PatchAction::SplitBlock {
            index: 5,
            cursor: doc.get_cursor(text, 5, None).unwrap(),
            conflict: false,
            parents: vec![
                "unordered-list-item".to_string(),
                "ordered-list-item".to_string()
            ],
            block_type: "ordered-list-item".to_string(),
        }
    );
}

#[test]
fn split_block_diff_full() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    let before = doc.get_heads();
    doc.split_block(
        &text,
        5,
        NewBlock::new("ordered-list-item")
            .with_parents(["unordered-list-item", "ordered-list-item"]),
    )
    .unwrap();
    let after = doc.get_heads();
    let patches = doc.diff(&before, &after);
    println!("{:?}", patches);
    assert_eq!(patches.len(), 1);
    assert_eq!(
        patches[0].action,
        PatchAction::SplitBlock {
            index: 5,
            cursor: doc.get_cursor(text, 5, None).unwrap(),
            conflict: false,
            parents: vec![
                "unordered-list-item".to_string(),
                "ordered-list-item".to_string()
            ],
            block_type: "ordered-list-item".to_string(),
        }
    );
}

#[test]
fn join_block_diff_incremental() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    doc.split_block(
        &text,
        5,
        NewBlock::new("ordered-list-item")
            .with_parents(["unordered-list-item", "ordered-list-item"]),
    )
    .unwrap();
    doc.update_diff_cursor();
    doc.join_block(&text, 5).unwrap();
    let patches = doc.diff_incremental();
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0].action, PatchAction::JoinBlock { index: 5 });
}

#[test]
fn join_block_diff_full() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    doc.split_block(
        &text,
        5,
        NewBlock::new("ordered-list-item")
            .with_parents(["unordered-list-item", "ordered-list-item"]),
    )
    .unwrap();
    let before = doc.get_heads();
    doc.join_block(&text, 5).unwrap();
    let after = doc.get_heads();
    let patches = doc.diff(&before, &after);
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0].action, PatchAction::JoinBlock { index: 5 });
}

#[test]
fn join_block_on_delete_incremental() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Helloo Wworld!").unwrap();
    doc.split_block(
        &text,
        6,
        NewBlock::new("ordered-list-item")
            .with_parents(["unordered-list-item", "ordered-list-item"]),
    )
    .unwrap();
    doc.update_diff_cursor();
    doc.delete(&text, 4).unwrap();
    doc.delete(&text, 4).unwrap();
    doc.delete(&text, 4).unwrap();
    doc.delete(&text, 4).unwrap();
    doc.delete(&text, 4).unwrap();
    let patches = doc.diff_incremental();
    assert_eq!(patches.len(), 3);
    assert_eq!(
        patches[0].action,
        PatchAction::DeleteSeq {
            index: 4,
            length: 2
        }
    );
    assert_eq!(patches[1].action, PatchAction::JoinBlock { index: 4 });
    assert_eq!(
        patches[2].action,
        PatchAction::DeleteSeq {
            index: 4,
            length: 2
        }
    );
}

#[test]
fn join_block_on_delete_full() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Helloo Wworld!").unwrap();
    doc.split_block(
        &text,
        6,
        NewBlock::new("ordered-list-item")
            .with_parents(["unordered-list-item", "ordered-list-item"]),
    )
    .unwrap();
    let before = doc.get_heads();
    doc.delete(&text, 4).unwrap();
    doc.delete(&text, 4).unwrap();
    doc.delete(&text, 4).unwrap();
    doc.delete(&text, 4).unwrap();
    doc.delete(&text, 4).unwrap();
    let after = doc.get_heads();
    let patches = doc.diff(&before, &after);
    assert_eq!(patches.len(), 3);
    assert_eq!(
        patches[0].action,
        PatchAction::DeleteSeq {
            index: 4,
            length: 2
        }
    );
    assert_eq!(patches[1].action, PatchAction::JoinBlock { index: 4 });
    assert_eq!(
        patches[2].action,
        PatchAction::DeleteSeq {
            index: 4,
            length: 2
        }
    );
}

#[test]
fn update_block_type_diff_incremental() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    doc.split_block(
        &text,
        5,
        NewBlock::new("ordered-list-item")
            .with_parents(["unordered-list-item", "ordered-list-item"]),
    )
    .unwrap();
    doc.update_diff_cursor();
    doc.update_block(
        &text,
        5,
        NewBlock::new("unordered-list-item").with_parents(["ordered-list-item"]),
    )
    .unwrap();
    let patches = doc.diff_incremental();
    assert_eq!(patches.len(), 1);
    assert_eq!(
        patches[0].action,
        PatchAction::UpdateBlock {
            index: 5,
            new_block_type: Some("unordered-list-item".to_string()),
            new_block_parents: Some(vec!["ordered-list-item".to_string()]),
        }
    );
}

#[test]
fn update_block_type_diff_incremental_add_parent() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    doc.split_block(&text, 5, NewBlock::new("unordered-list-item"))
        .unwrap();
    doc.update_diff_cursor();
    doc.update_block(
        &text,
        5,
        NewBlock::new("unordered-list-item").with_parents(["ordered-list-item"]),
    )
    .unwrap();
    let patches = doc.diff_incremental();
    assert_eq!(patches.len(), 1);
    assert_eq!(
        patches[0].action,
        PatchAction::UpdateBlock {
            index: 5,
            new_block_type: None,
            new_block_parents: Some(vec!["ordered-list-item".to_string()]),
        }
    );
}

#[test]
fn update_block_type_diff_full() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    doc.split_block(
        &text,
        5,
        NewBlock::new("ordered-list-item")
            .with_parents(["unordered-list-item", "ordered-list-item"]),
    )
    .unwrap();
    let before = doc.get_heads();
    doc.update_block(
        &text,
        5,
        NewBlock::new("unordered-list-item").with_parents(["ordered-list-item"]),
    )
    .unwrap();
    let after = doc.get_heads();
    println!("-------------------------");
    let patches = doc.diff(&before, &after);
    println!("{:?}", patches);
    assert_eq!(patches.len(), 1);
    assert_eq!(
        patches[0].action,
        PatchAction::UpdateBlock {
            index: 5,
            new_block_type: Some("unordered-list-item".to_string()),
            new_block_parents: Some(vec!["ordered-list-item".to_string()]),
        }
    );
}

#[test]
fn splitblock_merge_patches_incremental() {
    let mut doc1 = automerge::AutoCommit::new();
    let text = doc1.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc1.splice_text(&text, 0, 0, "Hello, World!").unwrap();

    let mut doc2 = doc1.fork();
    doc2.update_diff_cursor();

    doc1.split_block(&text, 6, NewBlock::new("paragraph"))
        .unwrap();
    doc2.merge(&mut doc1).unwrap();

    let patches = doc2.diff_incremental();
    println!("{:?}", patches);
    assert_eq!(patches.len(), 1);
    assert_eq!(
        patches[0].action,
        PatchAction::SplitBlock {
            index: 6,
            cursor: doc2.get_cursor(text, 6, None).unwrap(),
            conflict: false,
            parents: vec![],
            block_type: "paragraph".to_string(),
        }
    );
}

#[test]
fn splitblock_merge_patches_full() {
    let mut doc1 = automerge::AutoCommit::new();
    let text = doc1.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc1.splice_text(&text, 0, 0, "Hello, World!").unwrap();

    let mut doc2 = doc1.fork();
    let heads_before = doc2.get_heads();

    doc1.split_block(&text, 6, NewBlock::new("paragraph"))
        .unwrap();
    doc2.merge(&mut doc1).unwrap();

    doc2.update_diff_cursor();

    let heads_after = doc2.get_heads();
    let patches = doc2.diff(&heads_before, &heads_after);
    println!("{:?}", patches);
    assert_eq!(patches.len(), 1);
    assert_eq!(
        patches[0].action,
        PatchAction::SplitBlock {
            index: 6,
            cursor: doc2.get_cursor(text, 6, None).unwrap(),
            conflict: false,
            parents: vec![],
            block_type: "paragraph".to_string(),
        }
    );
}

#[test]
fn update_block_merge_patches_incremental() {
    let mut doc1 = automerge::AutoCommit::new();
    let text = doc1.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc1.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    doc1.split_block(&text, 6, NewBlock::new("paragraph"))
        .unwrap();

    let mut doc2 = doc1.fork();

    doc1.update_block(
        &text,
        6,
        NewBlock::new("unordered-list-item").with_parents(["ordered-list-item"]),
    )
    .unwrap();

    doc2.update_diff_cursor();
    doc2.merge(&mut doc1).unwrap();

    let patches = doc2.diff_incremental();
    assert_eq!(patches.len(), 1);
    assert_eq!(
        patches[0].action,
        PatchAction::UpdateBlock {
            index: 6,
            new_block_type: Some("unordered-list-item".to_string()),
            new_block_parents: Some(vec!["ordered-list-item".to_string()]),
        }
    );
}

#[test]
fn update_block_merge_patches_full() {
    let mut doc1 = automerge::AutoCommit::new();
    let text = doc1.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc1.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    doc1.split_block(&text, 6, NewBlock::new("paragraph"))
        .unwrap();

    let mut doc2 = doc1.fork();

    doc1.update_block(
        &text,
        6,
        NewBlock::new("unordered-list-item").with_parents(["ordered-list-item"]),
    )
    .unwrap();

    let heads_before = doc2.get_heads();
    doc2.merge(&mut doc1).unwrap();
    let heads_after = doc2.get_heads();

    doc2.update_diff_cursor();

    let patches = doc2.diff(&heads_before, &heads_after);
    println!("{:?}", patches);
    assert_eq!(patches.len(), 1);
    assert_eq!(
        patches[0].action,
        PatchAction::UpdateBlock {
            index: 6,
            new_block_type: Some("unordered-list-item".to_string()),
            new_block_parents: Some(vec!["ordered-list-item".to_string()]),
        }
    );
}

#[test]
fn join_block_merge_patches_incremental() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    doc.split_block(
        &text,
        5,
        NewBlock::new("ordered-list-item")
            .with_parents(["unordered-list-item", "ordered-list-item"]),
    )
    .unwrap();

    let mut doc2 = doc.fork();

    doc.join_block(&text, 5).unwrap();

    doc2.update_diff_cursor();
    doc2.merge(&mut doc).unwrap();
    let patches = doc2.diff_incremental();
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0].action, PatchAction::JoinBlock { index: 5 });
}

#[test]
fn join_block_merge_patches_full() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    doc.split_block(
        &text,
        5,
        NewBlock::new("ordered-list-item")
            .with_parents(["unordered-list-item", "ordered-list-item"]),
    )
    .unwrap();

    let mut doc2 = doc.fork();
    let heads_before = doc2.get_heads();

    doc.join_block(&text, 5).unwrap();

    doc2.merge(&mut doc).unwrap();
    let heads_after = doc2.get_heads();
    doc2.update_diff_cursor();
    let patches = doc2.diff(&heads_before, &heads_after);
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0].action, PatchAction::JoinBlock { index: 5 });
}

#[test]
fn split_block_at_end_of_document_incremental() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "item 1").unwrap();
    doc.split_block(&text, 0, NewBlock::new("list_item"))
        .unwrap();
    doc.update_diff_cursor();
    doc.split_block(&text, 7, NewBlock::new("list_item"))
        .unwrap();

    let patches = doc.diff_incremental();
    assert_eq!(patches.len(), 1);
    assert_eq!(
        patches[0].action,
        PatchAction::SplitBlock {
            index: 7,
            cursor: doc.get_cursor(text, 7, None).unwrap(),
            conflict: false,
            parents: vec![],
            block_type: "list_item".to_string(),
        }
    );
}

#[test]
fn split_block_at_end_of_document_full() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.split_block(&text, 0, NewBlock::new("paragraph"))
        .unwrap();
    doc.splice_text(&text, 1, 0, "item 1").unwrap();
    doc.update_block(&text, 0, NewBlock::new("list_item"))
        .unwrap();
    let heads_before = doc.get_heads();
    doc.split_block(&text, 7, NewBlock::new("list_item"))
        .unwrap();
    let heads_after = doc.get_heads();
    doc.update_diff_cursor();

    let patches = doc.diff(&heads_before, &heads_after);
    assert_eq!(patches.len(), 1);
    assert_eq!(
        patches[0].action,
        PatchAction::SplitBlock {
            index: 7,
            cursor: doc.get_cursor(text, 7, None).unwrap(),
            conflict: false,
            parents: vec![],
            block_type: "list_item".to_string(),
        }
    );
}

#[test]
fn update_blocks_change_block_type() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.split_block(&text, 0, NewBlock::new("ordered-list-item"))
        .unwrap();
    doc.splice_text(&text, 1, 0, "item 1").unwrap();
    doc.split_block(&text, 7, NewBlock::new("ordered-list-item"))
        .unwrap();
    doc.splice_text(&text, 8, 0, "item 2").unwrap();

    doc.update_diff_cursor();

    doc.update_blocks(
        &text,
        [
            BlockOrText::Block(Block::new("paragraph".to_string())),
            BlockOrText::Text("item 1".into()),
            BlockOrText::Block(
                Block::new("unordered-list-item".to_string())
                    .with_parents(vec!["ordered-list-item".to_string()]),
            ),
            BlockOrText::Text("item 2".into()),
        ],
    )
    .unwrap();

    let patches = doc.diff_incremental();

    assert_eq!(patches.len(), 2, "expected 2 patches, got {:?}", patches);

    let patch1 = patches[0].clone();
    assert_eq!(patch1.obj, text);
    let action @ PatchAction::UpdateBlock { .. } = patch1.action else {
        panic!(
            "expected first patch to be an update block, got {:?}",
            patch1
        );
    };
    assert_eq!(
        action,
        PatchAction::UpdateBlock {
            index: 0,
            new_block_type: Some("paragraph".to_string()),
            new_block_parents: None,
        }
    );

    let patch2 = patches[1].clone();
    assert_eq!(patch2.obj, text);
    let action @ PatchAction::UpdateBlock { .. } = patch2.action else {
        panic!(
            "expected second patch to be an update block, got {:?}",
            patch2
        );
    };
    assert_eq!(
        action,
        PatchAction::UpdateBlock {
            index: 7,
            new_block_type: Some("unordered-list-item".to_string()),
            new_block_parents: Some(vec!["ordered-list-item".to_string()]),
        }
    );
}

#[test]
fn update_blocks_updates_text() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.split_block(&text, 0, NewBlock::new("ordered-list-item"))
        .unwrap();
    doc.splice_text(&text, 1, 0, "first thing").unwrap();
    doc.split_block(&text, 12, NewBlock::new("ordered-list-item"))
        .unwrap();
    doc.splice_text(&text, 13, 0, "second thing").unwrap();
    let spans_before = doc.spans(&text).unwrap().collect::<Vec<_>>();

    doc.update_diff_cursor();

    doc.update_blocks(
        &text,
        [
            BlockOrText::Block(Block::new("ordered-list-item".to_string())),
            BlockOrText::Text("the first thing".into()),
            BlockOrText::Block(Block::new("paragraph".to_string())),
            BlockOrText::Text("the things are done".into()),
        ],
    )
    .unwrap();

    let patches = doc.diff_incremental();
    let split_block_patches = patches
        .iter()
        .filter(|p| matches!(p.action, PatchAction::SplitBlock { .. }))
        .count();
    assert_eq!(split_block_patches, 0, "expected no split block patches");

    let update_block_patches = patches
        .iter()
        .filter_map(|p| match &p.action {
            action @ PatchAction::UpdateBlock { .. } => Some(p),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        update_block_patches.len(),
        1,
        "expected one update block patches"
    );
    assert_eq!(
        update_block_patches[0].action,
        PatchAction::UpdateBlock {
            index: 16,
            new_block_type: Some("paragraph".to_string()),
            new_block_parents: None,
        }
    );

    let num_splice_or_del_patches = patches
        .iter()
        .filter_map(|p| match &p.action {
            action @ PatchAction::SpliceText { .. } => Some(p),
            action @ PatchAction::DeleteSeq { .. } => Some(p),
            _ => None,
        })
        .count();
    assert_eq!(
        patches.len() - update_block_patches.len(),
        num_splice_or_del_patches,
        "expected one update patch and the rest to be splice or delete patches"
    );
}

#[test]
fn update_blocks_noop() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.split_block(&text, 0, NewBlock::new("ordered-list-item"))
        .unwrap();
    doc.splice_text(&text, 1, 0, "item 1").unwrap();
    let heads_before = doc.get_heads();

    doc.update_diff_cursor();

    doc.update_blocks(
        &text,
        [
            BlockOrText::Block(Block::new("ordered-list-item".to_string())),
            BlockOrText::Text("item 1".into()),
        ],
    )
    .unwrap();
    let heads_after = doc.get_heads();

    let patches = doc.diff_incremental();
    assert_eq!(patches.len(), 0, "expected no patches");

    let patches = doc.diff(&heads_before, &heads_after);
    assert_eq!(patches.len(), 0, "expected no patches");
}

fn print_spans<I: Iterator<Item = automerge::iter::Span>>(spans: I) {
    for span in spans {
        match span {
            automerge::iter::Span::Block(block) => {
                use automerge::hydrate::Value;
                let Value::Map(mut map) = block else {
                    panic!("expected block to be a map, got {:?}", block);
                };
                let Some(Value::Scalar(automerge::ScalarValue::Str(s))) = map.get("type").clone()
                else {
                    panic!("expected block to have a type, got {:?}", map);
                };
                let block_type = s.to_string();
                let Value::List(parents) = map.get("parents").unwrap() else {
                    panic!("expected block to have parents, got {:?}", map);
                };
                let parents = parents
                    .iter()
                    .map(|v| match &v.value {
                        Value::Scalar(automerge::ScalarValue::Str(s)) => s.to_string(),
                        _ => panic!("expected parents to be values, got {:?}", v),
                    })
                    .collect::<Vec<_>>();
                println!("block: {:?}, parents: {:?}", block_type, parents);
            }
            automerge::iter::Span::Text(s, _) => {
                println!("text: {:?}", s);
            }
        }
    }
}

fn print_patches(patches: Vec<automerge::Patch>) {
    for patch in patches {
        match patch.action {
            PatchAction::PutMap {
                key,
                value,
                conflict,
            } => println!("put map: {:?}", key),
            PatchAction::PutSeq {
                index,
                value,
                conflict,
            } => println!("put seq: {:?}", index),
            PatchAction::Insert { index, values } => {
                let values = values
                    .iter()
                    .map(|(val, _, _)| match val {
                        automerge::Value::Object(o) => "object".to_string(),
                        automerge::Value::Scalar(v) => format!("{:?}", v),
                    })
                    .collect::<Vec<_>>();
                println!("insert: {:?}: {:?}", index, values);
            }
            PatchAction::SpliceText {
                index,
                value,
                marks,
            } => println!("splice text at {:?}: '{}'", index, value.make_string()),
            PatchAction::Increment { prop, value } => {
                println!("increment: {:?} by {}", prop, value)
            }
            PatchAction::Conflict { prop } => println!("mark conflict: {:?}", prop),
            PatchAction::DeleteMap { key } => println!("delete map: {:?}", key),
            PatchAction::DeleteSeq { index, length } => {
                println!("delete seq: {:?} for {:?}", index, length)
            }
            PatchAction::Mark { marks } => println!("mark: {:?}", marks),
            PatchAction::SplitBlock {
                index,
                cursor,
                conflict,
                parents,
                block_type,
            } => println!(
                "split block at {:?} with type {:?} and parents {:?}",
                index, block_type, parents
            ),
            PatchAction::JoinBlock { index } => println!("join block at {:?}", index),
            PatchAction::UpdateBlock {
                index,
                new_block_type,
                new_block_parents,
            } => println!(
                "update block at {:?} with type {:?} and parents {:?}",
                index, new_block_type, new_block_parents
            ),
        }
    }
}
