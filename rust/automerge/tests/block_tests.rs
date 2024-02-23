use automerge::{sync::SyncDoc, transaction::Transactable, ObjType, PatchAction, ReadDoc, ROOT};

#[test]
fn split_block_diff_incremental() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    doc.update_diff_cursor();
    let _block = doc.split_block(&text, 5, "ordered-list-item", &["unordered-list-item", "ordered-list-item"]).unwrap();
    let patches = doc.diff_incremental();
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0].action, PatchAction::SplitBlock {
        index: 5,
        cursor: doc.get_cursor(text, 5, None).unwrap(),
        conflict: false,
        parents: vec!["unordered-list-item".to_string(), "ordered-list-item".to_string()],
        block_type: "ordered-list-item".to_string(),
    });
}

#[test]
fn split_block_diff_full() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    let before = doc.get_heads();
    doc.split_block(&text, 5, "ordered-list-item", &["unordered-list-item", "ordered-list-item"]).unwrap();
    let after = doc.get_heads();
    let patches = doc.diff(&before, &after);
    println!("{:?}", patches);
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0].action, PatchAction::SplitBlock {
        index: 5,
        cursor: doc.get_cursor(text, 5, None).unwrap(),
        conflict: false,
        parents: vec!["unordered-list-item".to_string(), "ordered-list-item".to_string()],
        block_type: "ordered-list-item".to_string(),
    });
}

#[test]
fn join_block_diff_incremental() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    doc.split_block(&text, 5, "ordered-list-item", &["unordered-list-item", "ordered-list-item"]).unwrap();
    doc.update_diff_cursor();
    doc.join_block(&text, 5).unwrap();
    let patches = doc.diff_incremental();
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0].action, PatchAction::JoinBlock {
        index: 5,
    });
}

#[test]
fn join_block_diff_full() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    doc.split_block(&text, 5, "ordered-list-item", &["unordered-list-item", "ordered-list-item"]).unwrap();
    let before = doc.get_heads();
    doc.join_block(&text, 5).unwrap();
    let after = doc.get_heads();
    let patches = doc.diff(&before, &after);
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0].action, PatchAction::JoinBlock {
        index: 5,
    });
}

#[test]
fn join_block_on_delete_incremental() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Helloo Wworld!").unwrap();
    doc.split_block(&text, 6, "ordered-list-item", &["unordered-list-item", "ordered-list-item"]).unwrap();
    doc.update_diff_cursor();
    doc.delete(&text, 4).unwrap();
    doc.delete(&text, 4).unwrap();
    doc.delete(&text, 4).unwrap();
    doc.delete(&text, 4).unwrap();
    doc.delete(&text, 4).unwrap();
    let patches = doc.diff_incremental();
    assert_eq!(patches.len(), 3);
    assert_eq!(patches[0].action, PatchAction::DeleteSeq { index: 4, length: 2 });
    assert_eq!(patches[1].action, PatchAction::JoinBlock {
        index: 4,
    });
    assert_eq!(patches[2].action, PatchAction::DeleteSeq { index: 4, length: 2 });
}

#[test]
fn join_block_on_delete_full() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Helloo Wworld!").unwrap();
    doc.split_block(&text, 6, "ordered-list-item", &["unordered-list-item", "ordered-list-item"]).unwrap();
    let before = doc.get_heads();
    doc.delete(&text, 4).unwrap();
    doc.delete(&text, 4).unwrap();
    doc.delete(&text, 4).unwrap();
    doc.delete(&text, 4).unwrap();
    doc.delete(&text, 4).unwrap();
    let after = doc.get_heads();
    let patches = doc.diff(&before, &after);
    assert_eq!(patches.len(), 3);
    assert_eq!(patches[0].action, PatchAction::DeleteSeq { index: 4, length: 2 });
    assert_eq!(patches[1].action, PatchAction::JoinBlock {
        index: 4,
    });
    assert_eq!(patches[2].action, PatchAction::DeleteSeq { index: 4, length: 2 });
}

#[test]
fn update_block_type_diff_incremental() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    doc.split_block(&text, 5, "ordered-list-item", &["unordered-list-item", "ordered-list-item"]).unwrap();
    doc.update_diff_cursor();
    doc.update_block(&text, 5, "unordered-list-item", &["ordered-list-item"]).unwrap();
    let patches = doc.diff_incremental();
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0].action, PatchAction::UpdateBlock {
        index: 5,
        new_block_type: Some("unordered-list-item".to_string()),
        new_block_parents: Some(vec!["ordered-list-item".to_string()]),
    });
}

#[test]
fn update_block_type_diff_incremental_add_parent() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    doc.split_block(&text, 5, "unordered-list-item", &[]).unwrap();
    doc.update_diff_cursor();
    doc.update_block(&text, 5, "unordered-list-item", &["ordered-list-item"]).unwrap();
    let patches = doc.diff_incremental();
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0].action, PatchAction::UpdateBlock {
        index: 5,
        new_block_type: None,
        new_block_parents: Some(vec!["ordered-list-item".to_string()]),
    });
}

#[test]
fn update_block_type_diff_full() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    doc.split_block(&text, 5, "ordered-list-item", &["unordered-list-item", "ordered-list-item"]).unwrap();
    let before = doc.get_heads();
    doc.update_block(&text, 5, "unordered-list-item", &["ordered-list-item"]).unwrap();
    let after = doc.get_heads();
    println!("-------------------------");
    let patches = doc.diff(&before, &after);
    println!("{:?}", patches);
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0].action, PatchAction::UpdateBlock {
        index: 5,
        new_block_type: Some("unordered-list-item".to_string()),
        new_block_parents: Some(vec!["ordered-list-item".to_string()]),
    });
    
}

#[test]
fn splitblock_merge_patches_incremental() {
    let mut doc1 = automerge::AutoCommit::new();
    let text = doc1.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc1.splice_text(&text, 0, 0, "Hello, World!").unwrap();

    let mut doc2 = doc1.fork();
    doc2.update_diff_cursor();

    doc1.split_block(&text, 6, "paragraph", &[]).unwrap();
    doc2.merge(&mut doc1).unwrap();

    let patches = doc2.diff_incremental();
    println!("{:?}", patches);
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0].action, PatchAction::SplitBlock {
        index: 6,
        cursor: doc2.get_cursor(text, 6, None).unwrap(),
        conflict: false,
        parents: vec![],
        block_type: "paragraph".to_string(),
    });
}

#[test]
fn splitblock_merge_patches_full() {
    let mut doc1 = automerge::AutoCommit::new();
    let text = doc1.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc1.splice_text(&text, 0, 0, "Hello, World!").unwrap();

    let mut doc2 = doc1.fork();
    let heads_before = doc2.get_heads();

    doc1.split_block(&text, 6, "paragraph", &[]).unwrap();
    doc2.merge(&mut doc1).unwrap();

    doc2.update_diff_cursor();

    let heads_after = doc2.get_heads();
    let patches = doc2.diff(&heads_before, &heads_after);
    println!("{:?}", patches);
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0].action, PatchAction::SplitBlock {
        index: 6,
        cursor: doc2.get_cursor(text, 6, None).unwrap(),
        conflict: false,
        parents: vec![],
        block_type: "paragraph".to_string(),
    });
}

#[test]
fn update_block_merge_patches_incremental() {
    let mut doc1 = automerge::AutoCommit::new();
    let text = doc1.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc1.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    doc1.split_block(&text, 6, "paragraph", &[]).unwrap();

    let mut doc2 = doc1.fork();

    doc1.update_block(&text, 6, "unordered-list-item", &["ordered-list-item"]).unwrap();

    doc2.update_diff_cursor();
    doc2.merge(&mut doc1).unwrap();

    let patches = doc2.diff_incremental();
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0].action, PatchAction::UpdateBlock {
        index: 6,
        new_block_type: Some("unordered-list-item".to_string()),
        new_block_parents: Some(vec!["ordered-list-item".to_string()]),
    });
}

#[test]
fn update_block_merge_patches_full() {
    let mut doc1 = automerge::AutoCommit::new();
    let text = doc1.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc1.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    doc1.split_block(&text, 6, "paragraph", &[]).unwrap();

    let mut doc2 = doc1.fork();

    doc1.update_block(&text, 6, "unordered-list-item", &["ordered-list-item"]).unwrap();

    let heads_before = doc2.get_heads();
    doc2.merge(&mut doc1).unwrap();
    let heads_after = doc2.get_heads();

    doc2.update_diff_cursor();

    let patches = doc2.diff(&heads_before, &heads_after);
    println!("{:?}", patches);
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0].action, PatchAction::UpdateBlock {
        index: 6,
        new_block_type: Some("unordered-list-item".to_string()),
        new_block_parents: Some(vec!["ordered-list-item".to_string()]),
    });
}

#[test]
fn join_block_merge_patches_incremental() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    doc.split_block(&text, 5, "ordered-list-item", &["unordered-list-item", "ordered-list-item"]).unwrap();
    
    let mut doc2 = doc.fork();

    doc.join_block(&text, 5).unwrap();

    doc2.update_diff_cursor();
    doc2.merge(&mut doc).unwrap();
    let patches = doc2.diff_incremental();
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0].action, PatchAction::JoinBlock {
        index: 5,
    });
}

#[test]
fn join_block_merge_patches_full() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Hello, World!").unwrap();
    doc.split_block(&text, 5, "ordered-list-item", &["unordered-list-item", "ordered-list-item"]).unwrap();
    
    let mut doc2 = doc.fork();
    let heads_before = doc2.get_heads();

    doc.join_block(&text, 5).unwrap();

    doc2.merge(&mut doc).unwrap();
    let heads_after = doc2.get_heads();
    doc2.update_diff_cursor();
    let patches = doc2.diff(&heads_before, &heads_after);
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0].action, PatchAction::JoinBlock {
        index: 5,
    });
}
