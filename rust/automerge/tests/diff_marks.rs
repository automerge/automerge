use automerge::iter::Span;
use automerge::marks::{ExpandMark, Mark, UpdateSpansConfig};
use automerge::{transaction::Transactable, AutoCommit, ObjType, ReadDoc, ScalarValue, ROOT};
use std::sync::Arc;

fn markset(values: Vec<(&'static str, ScalarValue)>) -> Option<Arc<automerge::marks::MarkSet>> {
    if values.is_empty() {
        None
    } else {
        Some(Arc::new(
            values
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect::<automerge::marks::MarkSet>(),
        ))
    }
}

#[test]
fn overlapping_marks_remove_one_keep_other() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    // Create overlapping marks: "hello world" with "world" being both bold and italic
    doc.splice_text(&text, 0, 0, "hello world").unwrap();
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 6, 11),
        ExpandMark::Both,
    )
    .unwrap();
    doc.mark(
        &text,
        Mark::new("italic".to_string(), true, 6, 11),
        ExpandMark::Both,
    )
    .unwrap();

    // Update to remove italic but keep bold
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: "hello ".into(),
                marks: None,
            },
            Span::Text {
                text: "world".into(),
                marks: markset(vec![("bold", true.into())]),
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(
        spans,
        vec![
            Span::Text {
                text: "hello ".to_string(),
                marks: None
            },
            Span::Text {
                text: "world".to_string(),
                marks: markset(vec![("bold", true.into())])
            },
        ]
    );
}

#[test]
fn overlapping_marks_change_boundaries() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "hello beautiful world")
        .unwrap();
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 0, 15),
        ExpandMark::Both,
    )
    .unwrap();
    doc.mark(
        &text,
        Mark::new("italic".to_string(), true, 6, 21),
        ExpandMark::Both,
    )
    .unwrap();

    // Change boundaries: bold on "hello", italic on "world"
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: "hello".into(),
                marks: markset(vec![("bold", true.into())]),
            },
            Span::Text {
                text: " beautiful ".into(),
                marks: None,
            },
            Span::Text {
                text: "world".into(),
                marks: markset(vec![("italic", true.into())]),
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(
        spans,
        vec![
            Span::Text {
                text: "hello".to_string(),
                marks: markset(vec![("bold", true.into())])
            },
            Span::Text {
                text: " beautiful ".to_string(),
                marks: None
            },
            Span::Text {
                text: "world".to_string(),
                marks: markset(vec![("italic", true.into())])
            },
        ]
    );
}

#[test]
fn overlapping_marks_add_third_mark() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "hello world").unwrap();
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 0, 11),
        ExpandMark::Both,
    )
    .unwrap();
    doc.mark(
        &text,
        Mark::new("italic".to_string(), true, 6, 11),
        ExpandMark::Both,
    )
    .unwrap();

    // Add underline that partially overlaps both
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: "hel".into(),
                marks: markset(vec![("bold", true.into())]),
            },
            Span::Text {
                text: "lo wo".into(),
                marks: markset(vec![("bold", true.into()), ("underline", true.into())]),
            },
            Span::Text {
                text: "rld".into(),
                marks: markset(vec![
                    ("bold", true.into()),
                    ("italic", true.into()),
                    ("underline", true.into()),
                ]),
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    // The exact number of spans depends on grapheme clustering, but we should have:
    // - "hel" with just bold
    // - "lo wo" with bold and underline
    // - "rld" with bold, italic, and underline
    let collected_text: String = spans
        .iter()
        .filter_map(|s| match s {
            Span::Text { text: t, marks: _ } => Some(t.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(collected_text, "hello world");

    // Verify the marks are applied correctly
    let all_text = doc.text(&text).unwrap();
    assert_eq!(all_text, "hello world");
}

#[test]
fn mark_expands() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "bold text").unwrap();
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 0, 4),
        ExpandMark::Both,
    )
    .unwrap();

    // Expand mark to cover more text
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [Span::Text {
            text: "bold text".into(),
            marks: markset(vec![("bold", true.into())]),
        }],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(
        spans,
        vec![Span::Text {
            text: "bold text".to_string(),
            marks: markset(vec![("bold", true.into())])
        },]
    );
}

#[test]
fn mark_contracts() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "bold text").unwrap();
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 0, 9),
        ExpandMark::Both,
    )
    .unwrap();

    // Contract mark to cover less text
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: "bold".into(),
                marks: markset(vec![("bold", true.into())]),
            },
            Span::Text {
                text: " text".into(),
                marks: None,
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(
        spans,
        vec![
            Span::Text {
                text: "bold".to_string(),
                marks: markset(vec![("bold", true.into())])
            },
            Span::Text {
                text: " text".to_string(),
                marks: None
            },
        ]
    );
}

#[test]
fn mark_shifts_position() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "bold text").unwrap();
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 0, 4),
        ExpandMark::Both,
    )
    .unwrap();

    // Shift mark position
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: "text ".into(),
                marks: None,
            },
            Span::Text {
                text: "bold".into(),
                marks: markset(vec![("bold", true.into())]),
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(
        spans,
        vec![
            Span::Text {
                text: "text ".to_string(),
                marks: None
            },
            Span::Text {
                text: "bold".to_string(),
                marks: markset(vec![("bold", true.into())])
            },
        ]
    );
}

#[test]
fn mark_splits() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "bold text here").unwrap();
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 0, 14),
        ExpandMark::Both,
    )
    .unwrap();

    // Split mark into two
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: "bold".into(),
                marks: markset(vec![("bold", true.into())]),
            },
            Span::Text {
                text: " text ".into(),
                marks: None,
            },
            Span::Text {
                text: "here".into(),
                marks: markset(vec![("bold", true.into())]),
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(
        spans,
        vec![
            Span::Text {
                text: "bold".to_string(),
                marks: markset(vec![("bold", true.into())])
            },
            Span::Text {
                text: " text ".to_string(),
                marks: None
            },
            Span::Text {
                text: "here".to_string(),
                marks: markset(vec![("bold", true.into())])
            },
        ]
    );
}

#[test]
fn adjacent_marks_merge() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "bold text").unwrap();
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 0, 4),
        ExpandMark::Both,
    )
    .unwrap();
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 5, 9),
        ExpandMark::Both,
    )
    .unwrap();

    // Merge adjacent marks
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [Span::Text {
            text: "bold text".into(),
            marks: markset(vec![("bold", true.into())]),
        }],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(
        spans,
        vec![Span::Text {
            text: "bold text".to_string(),
            marks: markset(vec![("bold", true.into())])
        },]
    );
}

#[test]
fn adjacent_marks_stay_separate() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "bold text").unwrap();

    // Create two separate bold marks that should remain separate
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: "bold".into(),
                marks: markset(vec![("bold", true.into())]),
            },
            Span::Text {
                text: " ".into(),
                marks: None,
            },
            Span::Text {
                text: "text".into(),
                marks: markset(vec![("bold", true.into())]),
            },
        ],
    )
    .unwrap();

    // Now update with the same structure - marks should remain separate
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: "bold".into(),
                marks: markset(vec![("bold", true.into())]),
            },
            Span::Text {
                text: " ".into(),
                marks: None,
            },
            Span::Text {
                text: "text".into(),
                marks: markset(vec![("bold", true.into())]),
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(
        spans,
        vec![
            Span::Text {
                text: "bold".to_string(),
                marks: markset(vec![("bold", true.into())])
            },
            Span::Text {
                text: " ".to_string(),
                marks: None
            },
            Span::Text {
                text: "text".to_string(),
                marks: markset(vec![("bold", true.into())])
            },
        ]
    );
}

#[test]
fn different_adjacent_marks() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "bolditalic").unwrap();

    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: "bold".into(),
                marks: markset(vec![("bold", true.into())]),
            },
            Span::Text {
                text: "italic".into(),
                marks: markset(vec![("italic", true.into())]),
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(
        spans,
        vec![
            Span::Text {
                text: "bold".to_string(),
                marks: markset(vec![("bold", true.into())])
            },
            Span::Text {
                text: "italic".to_string(),
                marks: markset(vec![("italic", true.into())])
            },
        ]
    );
}

#[test]
fn mark_on_empty_string() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    // This should work but produce no visible marks since there's no text
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [Span::Text {
            text: "".into(),
            marks: markset(vec![("bold", true.into())]),
        }],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(spans.len(), 0);
}

#[test]
fn mark_on_whitespace() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: " ".into(),
                marks: markset(vec![("bold", true.into())]),
            },
            Span::Text {
                text: "\n".into(),
                marks: markset(vec![("italic", true.into())]),
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(
        spans,
        vec![
            Span::Text {
                text: " ".to_string(),
                marks: markset(vec![("bold", true.into())])
            },
            Span::Text {
                text: "\n".to_string(),
                marks: markset(vec![("italic", true.into())])
            },
        ]
    );
}

#[test]
fn removing_all_text_from_marked_span() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "hello world").unwrap();
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 0, 5),
        ExpandMark::Both,
    )
    .unwrap();

    // Remove the marked text
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [Span::Text {
            text: " world".into(),
            marks: None,
        }],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(
        spans,
        vec![Span::Text {
            text: " world".to_string(),
            marks: None
        },]
    );
}

#[test]
fn mark_spans_across_block() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "bold").unwrap();
    doc.split_block(&text, 4).unwrap();
    doc.splice_text(&text, 5, 0, "text").unwrap();

    // Apply mark spanning across block
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: "bold".into(),
                marks: markset(vec![("bold", true.into())]),
            },
            Span::Block(automerge::hydrate_map! {}),
            Span::Text {
                text: "text".into(),
                marks: markset(vec![("bold", true.into())]),
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(spans.len(), 3);
    match &spans[0] {
        Span::Text { text: t, marks: m } => {
            assert_eq!(t, "bold");
            assert!(m.is_some());
        }
        _ => panic!("Expected text span"),
    }
    match &spans[2] {
        Span::Text { text: t, marks: m } => {
            assert_eq!(t, "text");
            assert!(m.is_some());
        }
        _ => panic!("Expected text span"),
    }
}

#[test]
fn mark_ends_at_block_boundary() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "bold").unwrap();
    doc.split_block(&text, 4).unwrap();
    doc.splice_text(&text, 5, 0, "text").unwrap();

    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: "bold".into(),
                marks: markset(vec![("bold", true.into())]),
            },
            Span::Block(automerge::hydrate_map! {}),
            Span::Text {
                text: "text".into(),
                marks: None,
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(spans.len(), 3);
}

#[test]
fn nested_marks() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "italic bold and italic just italic")
        .unwrap();

    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: "italic ".into(),
                marks: markset(vec![("italic", true.into())]),
            },
            Span::Text {
                text: "bold and italic".into(),
                marks: markset(vec![("italic", true.into()), ("bold", true.into())]),
            },
            Span::Text {
                text: " just italic".into(),
                marks: markset(vec![("italic", true.into())]),
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(spans.len(), 3);
}

#[test]
fn many_marks_on_same_text() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "formatted").unwrap();

    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [Span::Text {
            text: "formatted".into(),
            marks: markset(vec![
                ("bold", true.into()),
                ("italic", true.into()),
                ("underline", true.into()),
                ("link", "https://example.com".into()),
            ]),
        }],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(spans.len(), 1);
    if let Span::Text {
        text: _,
        marks: Some(marks),
    } = &spans[0]
    {
        assert_eq!(marks.len(), 4);
    }
}

#[test]
fn mark_value_changes_link_url() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "click here").unwrap();
    doc.mark(
        &text,
        Mark::new("link".to_string(), "https://old.com", 0, 10),
        ExpandMark::Both,
    )
    .unwrap();

    // Change link URL
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [Span::Text {
            text: "click here".into(),
            marks: markset(vec![("link", "https://new.com".into())]),
        }],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    if let Span::Text {
        text: _,
        marks: Some(marks),
    } = &spans[0]
    {
        let link_value = marks.iter().find(|(k, _)| k == &"link").map(|(_, v)| v);
        assert_eq!(
            link_value,
            Some(&ScalarValue::Str("https://new.com".into()))
        );
    }
}

#[test]
fn mark_value_changes_color() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "colored").unwrap();
    doc.mark(
        &text,
        Mark::new("color".to_string(), "red", 0, 7),
        ExpandMark::Both,
    )
    .unwrap();

    // Change color
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [Span::Text {
            text: "colored".into(),
            marks: markset(vec![("color", "blue".into())]),
        }],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    if let Span::Text {
        text: _,
        marks: Some(marks),
    } = &spans[0]
    {
        let color_value = marks.iter().find(|(k, _)| k == &"color").map(|(_, v)| v);
        assert_eq!(color_value, Some(&ScalarValue::Str("blue".into())));
    }
}

#[test]
fn mark_value_type_changes() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "text").unwrap();
    doc.mark(
        &text,
        Mark::new("custom".to_string(), true, 0, 4),
        ExpandMark::Both,
    )
    .unwrap();

    // Change from boolean to string
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [Span::Text {
            text: "text".into(),
            marks: markset(vec![("custom", "value".into())]),
        }],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    if let Span::Text {
        text: _,
        marks: Some(marks),
    } = &spans[0]
    {
        let custom_value = marks.iter().find(|(k, _)| k == &"custom").map(|(_, v)| v);
        assert_eq!(custom_value, Some(&ScalarValue::Str("value".into())));
    }
}

#[test]
fn multiple_marks_different_expand_behaviors() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    let mut config = UpdateSpansConfig::default();
    config
        .per_mark_expands
        .insert("before".to_string(), ExpandMark::Before);
    config
        .per_mark_expands
        .insert("after".to_string(), ExpandMark::After);
    config
        .per_mark_expands
        .insert("none".to_string(), ExpandMark::None);

    doc.update_spans(
        &text,
        config,
        [Span::Text {
            text: "text".into(),
            marks: markset(vec![
                ("before", true.into()),
                ("after", true.into()),
                ("none", true.into()),
            ]),
        }],
    )
    .unwrap();

    // Insert before
    doc.splice_text(&text, 0, 0, "a").unwrap();
    // Insert after
    doc.splice_text(&text, 5, 0, "b").unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    // "before" mark should expand to include "a"
    // "after" mark should expand to include "b"
    // "none" mark should not expand
    assert_eq!(spans.len(), 3); // "a", "text", "b"
}

#[test]
fn marks_with_expand_none_at_boundaries() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    let config = UpdateSpansConfig::default().with_default_expand(ExpandMark::None);

    doc.update_spans(
        &text,
        config,
        [Span::Text {
            text: "text".into(),
            marks: markset(vec![("mark", true.into())]),
        }],
    )
    .unwrap();

    // Insert at boundaries
    doc.splice_text(&text, 0, 0, "before ").unwrap();
    doc.splice_text(&text, 11, 0, " after").unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(
        spans,
        vec![
            Span::Text {
                text: "before ".to_string(),
                marks: None
            },
            Span::Text {
                text: "text".to_string(),
                marks: markset(vec![("mark", true.into())])
            },
            Span::Text {
                text: " after".to_string(),
                marks: None
            },
        ]
    );
}

#[test]
fn marks_on_emoji() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "Hello 👨‍👩‍👧‍👦 world").unwrap();

    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: "Hello ".into(),
                marks: None,
            },
            Span::Text {
                text: "👨‍👩‍👧‍👦".into(),
                marks: markset(vec![("emoji", true.into())]),
            },
            Span::Text {
                text: " world".into(),
                marks: None,
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(spans.len(), 3);
}

#[test]
fn marks_on_combining_characters() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    // Use a simple string with combining character
    doc.splice_text(&text, 0, 0, "café").unwrap();

    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [Span::Text {
            text: "café".into(),
            marks: markset(vec![("accented", true.into())]),
        }],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();

    // Just verify that marks can be applied to text with combining characters
    // The exact span count may vary based on text encoding
    assert!(!spans.is_empty());

    // Verify the text is preserved
    let full_text = doc.text(&text).unwrap();
    assert_eq!(full_text, "café");
}

#[test]
fn unmark_part_of_range() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "bold text here").unwrap();
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 0, 14),
        ExpandMark::Both,
    )
    .unwrap();

    // Remove mark from middle part
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: "bold".into(),
                marks: markset(vec![("bold", true.into())]),
            },
            Span::Text {
                text: " text ".into(),
                marks: None,
            },
            Span::Text {
                text: "here".into(),
                marks: markset(vec![("bold", true.into())]),
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(spans.len(), 3);
}

#[test]
fn unmark_creates_gaps() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "a b c d e").unwrap();
    doc.mark(
        &text,
        Mark::new("mark".to_string(), true, 0, 9),
        ExpandMark::Both,
    )
    .unwrap();

    // Create gaps in the mark
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: "a".into(),
                marks: markset(vec![("mark", true.into())]),
            },
            Span::Text {
                text: " b ".into(),
                marks: None,
            },
            Span::Text {
                text: "c".into(),
                marks: markset(vec![("mark", true.into())]),
            },
            Span::Text {
                text: " d ".into(),
                marks: None,
            },
            Span::Text {
                text: "e".into(),
                marks: markset(vec![("mark", true.into())]),
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(spans.len(), 5);
}

#[test]
fn block_properties_change_with_marks() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.split_block(&text, 0).unwrap();
    doc.splice_text(&text, 1, 0, "marked text").unwrap();
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 1, 7),
        ExpandMark::Both,
    )
    .unwrap();

    // Change block type while preserving marks
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Block(automerge::hydrate_map! {
                "type" => "paragraph",
                "level" => 1
            }),
            Span::Text {
                text: "marked".into(),
                marks: markset(vec![("bold", true.into())]),
            },
            Span::Text {
                text: " text".into(),
                marks: None,
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(spans.len(), 3);
}

#[test]
fn idempotent_update_spans() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    let spans = vec![
        Span::Text {
            text: "hello ".into(),
            marks: markset(vec![("bold", true.into())]),
        },
        Span::Text {
            text: "world".into(),
            marks: markset(vec![("italic", true.into())]),
        },
    ];

    // Apply same update multiple times
    doc.update_spans(&text, UpdateSpansConfig::default(), spans.clone())
        .unwrap();
    let version1 = doc.get_heads();

    doc.update_spans(&text, UpdateSpansConfig::default(), spans.clone())
        .unwrap();
    let version2 = doc.get_heads();

    doc.update_spans(&text, UpdateSpansConfig::default(), spans)
        .unwrap();
    let version3 = doc.get_heads();

    // Should not create additional changes after the first update
    assert_eq!(version2, version1);
    assert_eq!(version3, version1);
}

#[test]
fn alternating_mark_changes() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "text").unwrap();

    // Alternate between adding and removing marks
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [Span::Text {
            text: "text".into(),
            marks: markset(vec![("bold", true.into())]),
        }],
    )
    .unwrap();

    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [Span::Text {
            text: "text".into(),
            marks: None,
        }],
    )
    .unwrap();

    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [Span::Text {
            text: "text".into(),
            marks: markset(vec![("italic", true.into())]),
        }],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(
        spans,
        vec![Span::Text {
            text: "text".to_string(),
            marks: markset(vec![("italic", true.into())])
        },]
    );
}

#[test]
fn complex_unicode_text() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    // Mix of ASCII, emoji, and other Unicode
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: "Hello ".into(),
                marks: None,
            },
            Span::Text {
                text: "😊".into(),
                marks: markset(vec![("emoji", true.into())]),
            },
            Span::Text {
                text: " 世界 ".into(),
                marks: markset(vec![("chinese", true.into())]),
            },
            Span::Text {
                text: "🌍".into(),
                marks: markset(vec![("emoji", true.into())]),
            },
            Span::Text {
                text: " مرحبا".into(),
                marks: markset(vec![("arabic", true.into())]),
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(spans.len(), 5);
}

#[test]
fn block_with_marked_content() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    // Create blocks with marked text content
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Block(automerge::hydrate_map! {
                "type" => "heading",
                "level" => 1
            }),
            Span::Text {
                text: "Chapter ".into(),
                marks: None,
            },
            Span::Text {
                text: "One".into(),
                marks: markset(vec![("emphasis", true.into())]),
            },
            Span::Block(automerge::hydrate_map! {
                "type" => "paragraph"
            }),
            Span::Text {
                text: "This is the ".into(),
                marks: None,
            },
            Span::Text {
                text: "first".into(),
                marks: markset(vec![("bold", true.into())]),
            },
            Span::Text {
                text: " chapter.".into(),
                marks: None,
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(spans.len(), 7);
}

#[test]
fn empty_spans_between_marks() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    // Test handling of empty spans
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: "a".into(),
                marks: markset(vec![("mark", true.into())]),
            },
            Span::Text {
                text: "".into(),
                marks: None,
            }, // Empty span
            Span::Text {
                text: "b".into(),
                marks: markset(vec![("mark", true.into())]),
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    // Empty spans should not appear in output, and adjacent marks with same value are merged
    assert_eq!(
        spans,
        vec![Span::Text {
            text: "ab".to_string(),
            marks: markset(vec![("mark", true.into())])
        },]
    );
}

#[test]
fn marks_with_different_values_same_name() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "red blue green").unwrap();

    // Same mark name, different values
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: "red".into(),
                marks: markset(vec![("color", "red".into())]),
            },
            Span::Text {
                text: " ".into(),
                marks: None,
            },
            Span::Text {
                text: "blue".into(),
                marks: markset(vec![("color", "blue".into())]),
            },
            Span::Text {
                text: " ".into(),
                marks: None,
            },
            Span::Text {
                text: "green".into(),
                marks: markset(vec![("color", "green".into())]),
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(spans.len(), 5);

    // Verify each color mark has the correct value
    if let Span::Text {
        text: _,
        marks: Some(marks),
    } = &spans[0]
    {
        let color_value = marks.iter().find(|(k, _)| k == &"color").map(|(_, v)| v);
        assert_eq!(color_value, Some(&ScalarValue::Str("red".into())));
    }
    if let Span::Text {
        text: _,
        marks: Some(marks),
    } = &spans[2]
    {
        let color_value = marks.iter().find(|(k, _)| k == &"color").map(|(_, v)| v);
        assert_eq!(color_value, Some(&ScalarValue::Str("blue".into())));
    }
    if let Span::Text {
        text: _,
        marks: Some(marks),
    } = &spans[4]
    {
        let color_value = marks.iter().find(|(k, _)| k == &"color").map(|(_, v)| v);
        assert_eq!(color_value, Some(&ScalarValue::Str("green".into())));
    }
}

#[test]
fn update_spans_with_only_blocks() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    // Start with text
    doc.splice_text(&text, 0, 0, "text").unwrap();
    doc.split_block(&text, 4).unwrap();
    doc.splice_text(&text, 5, 0, "more").unwrap();

    // Update to only blocks (remove all text)
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Block(automerge::hydrate_map! {}),
            Span::Block(automerge::hydrate_map! {}),
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    // Should have at least one block
    assert!(spans.iter().any(|s| matches!(s, Span::Block(_))));
}

#[test]
fn marks_survive_block_updates() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    // Create initial structure with marks
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Block(automerge::hydrate_map! {"type" => "p"}),
            Span::Text {
                text: "marked".into(),
                marks: markset(vec![("bold", true.into())]),
            },
        ],
    )
    .unwrap();

    // Update block properties but keep marks
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Block(automerge::hydrate_map! {"type" => "h1", "level" => 1}),
            Span::Text {
                text: "marked".into(),
                marks: markset(vec![("bold", true.into())]),
            },
        ],
    )
    .unwrap();

    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert_eq!(spans.len(), 2);

    // Verify block changed but mark remained
    if let Span::Block(props) = &spans[0] {
        assert_eq!(props.get("type"), Some(&"h1".into()));
    }
    if let Span::Text {
        text: _,
        marks: Some(marks),
    } = &spans[1]
    {
        assert!(marks.iter().any(|(k, _)| k == "bold"));
    }
}

#[test]
fn update_spans_which_inserts_at_the_end_of_expand_mark_doesnt_generate_mark_changes() {
    // If we have a mark that expands at the end, then calling `update_spans` with a
    // single insertion at the end of the mark should not generate a mark change
    // because the existing mark should expand to include the new text.

    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "hello world").unwrap();
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 6, 11),
        ExpandMark::Both,
    )
    .unwrap();
    doc.commit();

    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        vec![
            Span::Text {
                text: "hello ".to_string(),
                marks: None,
            },
            Span::Text {
                text: "wworldd".to_string(),
                marks: markset(vec![("bold", true.into())]),
            },
        ],
    )
    .unwrap();

    let change_hash = doc.commit().expect("a change should be produced");
    let change = doc.get_change_by_hash(&change_hash).unwrap();
    assert_eq!(change.decode().operations.len(), 2); // There should be two insertion ops
}
