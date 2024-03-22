use automerge::{
    hydrate_list, hydrate_map, transaction::Transactable, BlockOrText, ObjType, ReadDoc, ROOT,
};
use test_log::test;

#[test]
fn update_blocks_change_block_properties() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    //let block = doc.split_block(&text, 0, NewBlock::new("ordered-list-item"))
    //.unwrap();
    let block1 = doc.split_block(&text, 0).unwrap();
    doc.update_object(
        &block1,
        &hydrate_map! {
            "parents" => hydrate_list![],
            "type" => "ordered-list-item",
            "attrs" => hydrate_map!{}
        }
        .into(),
    )
    .unwrap();
    doc.splice_text(&text, 1, 0, "item 1").unwrap();
    //let block2 = doc.split_block(&text, 7, NewBlock::new("ordered-list-item"))
    let block2 = doc.split_block(&text, 7).unwrap();
    doc.update_object(
        &block2,
        &hydrate_map! {
            "parents" => hydrate_list![],
            "type" => "ordered-list-item",
            "attrs" => hydrate_map!{}
        }
        .into(),
    )
    .unwrap();
    doc.splice_text(&text, 8, 0, "item 2").unwrap();

    doc.update_diff_cursor();

    doc.update_blocks(
        &text,
        [
            BlockOrText::Block(hydrate_map! {
                "type" => "paragraph",
                "parents" => hydrate_list![],
                "attrs" => hydrate_map!{}
            }),
            BlockOrText::Text("item 1".into()),
            BlockOrText::Block(hydrate_map! {
                "type" => "unordered-list-item",
                "parents" => hydrate_list!["ordered-list-item"],
                "attrs" => hydrate_map!{
                    "key" => 1,
                },
            }),
            BlockOrText::Text("item 2".into()),
        ],
    )
    .unwrap();

    let spans = doc
        .spans(&text)
        .unwrap()
        .map(|s| match s {
            automerge::iter::Span::Block(b) => BlockOrText::Block(b),
            automerge::iter::Span::Text(t, _) => BlockOrText::Text(std::borrow::Cow::Owned(t)),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        spans,
        vec![
            BlockOrText::Block(
                hydrate_map! {
                    "type" => "paragraph",
                    "parents" => hydrate_list![],
                    "attrs" => hydrate_map!{}
                }
                .into()
            ),
            BlockOrText::Text("item 1".into()),
            BlockOrText::Block(
                hydrate_map! {
                    "type" => "unordered-list-item",
                    "parents" => hydrate_list!["ordered-list-item"],
                    "attrs" => hydrate_map!{"key" => 1}
                }
                .into()
            ),
            BlockOrText::Text("item 2".into()),
        ]
    );
}

#[test]
fn update_blocks_updates_text() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    let _block1 = doc.split_block(&text, 0).unwrap();
    doc.splice_text(&text, 1, 0, "first thing").unwrap();
    let _block2 = doc.split_block(&text, 12).unwrap();
    doc.splice_text(&text, 13, 0, "second thing").unwrap();

    doc.update_diff_cursor();

    doc.update_blocks(
        &text,
        [
            BlockOrText::Block(
                hydrate_map! {
                    "type" => "ordered-list-item",
                    "parents" => hydrate_list![],
                    "attrs" => hydrate_map!{}
                }
                .into(),
            ),
            BlockOrText::Text("the first thing".into()),
            BlockOrText::Block(
                hydrate_map! {
                    "type" => "paragraph",
                    "parents" => hydrate_list![],
                    "attrs" => hydrate_map!{}
                }
                .into(),
            ),
            BlockOrText::Text("the things are done".into()),
        ],
    )
    .unwrap();

    //let spans = doc.spans(&text).unwrap().collect::<Vec<_>>();
    let spans = doc
        .spans(&text)
        .unwrap()
        .map(|s| match s {
            automerge::iter::Span::Block(b) => BlockOrText::Block(b),
            automerge::iter::Span::Text(t, _) => BlockOrText::Text(std::borrow::Cow::Owned(t)),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        spans,
        vec![
            BlockOrText::Block(
                hydrate_map! {
                    "type" => "ordered-list-item",
                    "parents" => hydrate_list![],
                    "attrs" => hydrate_map!{}
                }
                .into()
            ),
            BlockOrText::Text("the first thing".into()),
            BlockOrText::Block(
                hydrate_map! {
                    "type" => "paragraph",
                    "parents" => hydrate_list![],
                    "attrs" => hydrate_map!{}
                }
                .into()
            ),
            BlockOrText::Text("the things are done".into()),
        ]
    );
}

#[test]
fn update_blocks_noop() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    let block1 = doc.split_block(&text, 0).unwrap();
    doc.update_object(
        &block1,
        &hydrate_map! {
            "parents" => hydrate_list![],
            "type" => "ordered-list-item",
            "attrs" => hydrate_map!{}
        }
        .into(),
    )
    .unwrap();
    doc.splice_text(&text, 1, 0, "item 1").unwrap();

    doc.update_diff_cursor();

    doc.update_blocks(
        &text,
        [
            BlockOrText::Block(
                hydrate_map! {
                    "type" => "ordered-list-item",
                    "parents" => hydrate_list![],
                    "attrs" => hydrate_map!{}
                }
                .into(),
            ),
            BlockOrText::Text("item 1".into()),
        ],
    )
    .unwrap();

    let patches = doc.diff_incremental();
    assert_eq!(patches.len(), 0, "expected no patches");
}

#[test]
fn update_blocks_updates_text_and_blocks_at_once() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    //let block1 = doc.split_block(&text, 0, NewBlock::new("paragraph"))
    let block1 = doc.split_block(&text, 0).unwrap();
    doc.update_object(
        &block1,
        &hydrate_map! {
            "parents" => hydrate_list![],
            "type" => "paragraph",
            "attrs" => hydrate_map!{}
        }
        .into(),
    )
    .unwrap();
    doc.splice_text(&text, 1, 0, "hello world").unwrap();

    doc.update_blocks(
        &text,
        vec![
            BlockOrText::Block(
                hydrate_map! {
                    "type" => "unordered-list-item",
                    "parents" => hydrate_list![],
                    "attrs" => hydrate_map!{}
                }
                .into(),
            ),
            BlockOrText::Text("goodbye world".into()),
        ],
    )
    .unwrap();

    let spans_after = doc.spans(&text).unwrap().collect::<Vec<_>>();
    assert_eq!(
        spans_after,
        vec![
            automerge::iter::Span::Block(
                hydrate_map! {
                    "type" => "unordered-list-item",
                    "parents" => hydrate_list![],
                    "attrs" => hydrate_map!{}
                }
                .into()
            ),
            automerge::iter::Span::Text("goodbye world".into(), None),
        ]
    );
}

#[test]
fn splice_patch_with_blocks_across_optree_page_boundary() {
    // Reproduces an issue where if you have blocks in the document and then insert text at the end
    // of the document, when you hit a multiple of the opetree page boundary the remote patches
    // (i.e. not the patches produced by TransactionInner) would be wrong
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    let block1 = doc.split_block(&text, 0).unwrap();
    doc.update_object(
        &block1,
        &hydrate_map! {
            "parents" => hydrate_list![],
            "type" => "ordered-list-item",
            "attrs" => hydrate_map!{}
        }
        .into(),
    )
    .unwrap();
    doc.splice_text(&text, 1, 0, "item 1").unwrap();
    let block2 = doc.split_block(&text, 7).unwrap();
    doc.update_object(
        &block2,
        &hydrate_map! {
            "parents" => hydrate_list![],
            "type" => "ordered-list-item",
            "attrs" => hydrate_map!{}
        }
        .into(),
    )
    .unwrap();
    let replaced = doc.replace_block(&text, 7).unwrap();
    doc.update_object(
        &replaced,
        &hydrate_map! {
            "parents" => hydrate_list![],
            "type" => "paragraph",
            "attrs" => hydrate_map!{}
        }
        .into(),
    )
    .unwrap();
    let text_len = doc.length(&text);

    for i in 0..100 {
        println!("patching at {}", i + text_len);
        doc.update_diff_cursor();
        let mut doc2 = doc.fork();
        doc2.update_diff_cursor();
        doc.splice_text(&text, text_len + i, 0, "a").unwrap();
        let local_diff = doc.diff_incremental();
        let heads_before = doc2.get_heads();
        doc2.merge(&mut doc).unwrap();
        doc2.reset_diff_cursor();
        let heads_after = doc2.get_heads();
        let remote_diff = doc2.diff(&heads_before, &heads_after);
        if remote_diff != local_diff {
            #[cfg(feature = "optree-visualisation")]
            println!("{}", doc.visualise_optree(None));
            println!("-------------------------");
            #[cfg(feature = "optree-visualisation")]
            println!("{}", doc2.visualise_optree(None));
        }
        assert_eq!(local_diff, remote_diff);
    }
}
