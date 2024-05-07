use unicode_segmentation::UnicodeSegmentation;

use crate::{
    clock::Clock,
    iter::{SpanInternal, SpansInternal},
    op_tree::OpTreeOpIter,
    text_value::TextValue,
    transaction::TransactionInner,
    Automerge, BlockOrText, ObjId as ExId, PatchLog, ReadDoc,
};
mod myers;
mod replace;
mod utils;

pub(crate) fn myers_diff<'a, S: AsRef<str>>(
    doc: &'a mut Automerge,
    tx: &'a mut TransactionInner,
    patch_log: &mut PatchLog,
    text_obj: &ExId,
    new: S,
) -> Result<(), crate::AutomergeError> {
    let old = doc.text(text_obj)?;
    let new = new.as_ref();
    let old_graphemes = old.graphemes(true).collect::<Vec<&str>>();
    let new_graphemes = new.graphemes(true).collect::<Vec<&str>>();
    let mut hook = TxHook {
        tx,
        doc,
        patch_log,
        obj: text_obj,
        idx: 0,
        old: &old_graphemes,
        new: &new_graphemes,
    };
    myers::diff(
        &mut hook,
        &old_graphemes,
        0..old_graphemes.len(),
        &new_graphemes,
        0..new_graphemes.len(),
    )
}

struct TxHook<'a> {
    doc: &'a mut Automerge,
    tx: &'a mut TransactionInner,
    patch_log: &'a mut PatchLog,
    old: &'a [&'a str],
    new: &'a [&'a str],
    obj: &'a ExId,
    idx: usize,
}

impl<'a> myers::DiffHook for TxHook<'a> {
    type Error = crate::AutomergeError;

    fn equal(
        &mut self,
        old_index: usize,
        _new_index: usize,
        len: usize,
    ) -> Result<(), Self::Error> {
        self.idx += self.old[old_index..old_index + len]
            .iter()
            .map(|c| TextValue::width(c))
            .sum::<usize>();
        Ok(())
    }

    fn replace(
        &mut self,
        old_index: usize,
        old_len: usize,
        new_index: usize,
        new_len: usize,
    ) -> Result<(), Self::Error> {
        let new_chars = self.new[new_index..new_index + new_len].concat();
        let deleted = self.old[old_index..old_index + old_len]
            .iter()
            .map(|s| TextValue::width(s))
            .sum::<usize>();
        self.tx.splice_text(
            self.doc,
            self.patch_log,
            self.obj,
            self.idx,
            deleted as isize,
            &new_chars,
        )?;
        self.idx += TextValue::width(&new_chars);
        Ok(())
    }

    fn finish(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn delete(
        &mut self,
        old_index: usize,
        old_len: usize,
        _new_index: usize,
    ) -> Result<(), Self::Error> {
        let deleted_len: usize = self.old[old_index..old_index + old_len]
            .iter()
            .map(|s| TextValue::width(s))
            .sum();
        self.tx.splice_text(
            self.doc,
            self.patch_log,
            self.obj,
            self.idx,
            deleted_len as isize,
            "",
        )?;
        Ok(())
    }

    fn insert(
        &mut self,
        _old_index: usize,
        new_index: usize,
        new_len: usize,
    ) -> Result<(), Self::Error> {
        let new_chars = self.new[new_index..new_index + new_len].concat();
        self.tx
            .splice_text(self.doc, self.patch_log, self.obj, self.idx, 0, &new_chars)?;
        self.idx += TextValue::width(&new_chars);
        Ok(())
    }
}

pub(crate) fn myers_block_diff<'a, 'b, I: IntoIterator<Item = BlockOrText<'b>>>(
    doc: &'a mut Automerge,
    tx: &'a mut TransactionInner,
    patch_log: &mut PatchLog,
    text_obj: &crate::ObjId,
    new: I,
) -> Result<(), crate::AutomergeError> {
    let text_obj_meta = doc.exid_to_obj(text_obj)?;
    let old = spans_as_grapheme(doc, &text_obj_meta.id, None)?;
    let new = block_or_text_as_grapheme(new.into_iter());
    let mut hook = replace::Replace::new(BlockDiffHook {
        tx,
        doc,
        patch_log,
        obj: text_obj,
        idx: 0,
        old: &old,
        new: &new,
    });
    //let mut hook = BlockDiffHook {
    //tx,
    //doc,
    //patch_log,
    //obj: text_obj,
    //idx: 0,
    //old: &old,
    //new: &new,
    //};
    myers::diff(&mut hook, &old, 0..old.len(), &new, 0..new.len())
}

struct BlockDiffHook<'a> {
    doc: &'a mut Automerge,
    tx: &'a mut TransactionInner,
    patch_log: &'a mut PatchLog,
    old: &'a [BlockOrGrapheme],
    new: &'a [BlockOrGrapheme],
    obj: &'a ExId,
    idx: usize,
}

#[derive(Debug, Clone, PartialEq)]
enum BlockOrGrapheme {
    Block(crate::hydrate::Map),
    Grapheme(String),
}

impl BlockOrGrapheme {
    fn width(&self) -> usize {
        match self {
            BlockOrGrapheme::Block(_) => 1,
            BlockOrGrapheme::Grapheme(g) => TextValue::width(g),
        }
    }
}

impl<'a> myers::DiffHook for BlockDiffHook<'a> {
    type Error = crate::AutomergeError;

    fn equal(
        &mut self,
        old_index: usize,
        _new_index: usize,
        len: usize,
    ) -> Result<(), Self::Error> {
        for i in 0..len {
            self.idx += self.old[old_index + i].width();
        }
        Ok(())
    }

    fn delete(
        &mut self,
        old_index: usize,
        old_len: usize,
        _new_index: usize,
    ) -> Result<(), Self::Error> {
        for i in old_index..old_index + old_len {
            match &self.old[i] {
                BlockOrGrapheme::Block(_) => {
                    self.tx
                        .join_block(self.doc, self.patch_log, self.obj, self.idx)?;
                }
                BlockOrGrapheme::Grapheme(_) => {
                    self.tx
                        .delete(self.doc, self.patch_log, self.obj, self.idx)?;
                }
            }
        }
        Ok(())
    }

    fn insert(
        &mut self,
        _old_index: usize,
        new_index: usize,
        new_len: usize,
    ) -> Result<(), Self::Error> {
        //accumulate runs of text and insert them. Insert blocks immediately
        let mut run = String::new();
        for i in new_index..new_index + new_len {
            match &self.new[i] {
                BlockOrGrapheme::Block(b) => {
                    if !run.is_empty() {
                        self.tx.splice_text(
                            self.doc,
                            self.patch_log,
                            self.obj,
                            self.idx,
                            0,
                            &run,
                        )?;
                        self.idx += TextValue::width(&run);
                        run.clear();
                    }
                    split_block(self.doc, self.tx, self.patch_log, self.obj, self.idx, b)?;
                    self.idx += 1;
                }
                BlockOrGrapheme::Grapheme(g) => {
                    run.push_str(g);
                }
            }
        }
        if !run.is_empty() {
            self.tx
                .splice_text(self.doc, self.patch_log, self.obj, self.idx, 0, &run)?;
            self.idx += TextValue::width(&run);
        }
        Ok(())
    }

    fn replace(
        &mut self,
        old_index: usize,
        old_len: usize,
        new_index: usize,
        new_len: usize,
    ) -> Result<(), Self::Error> {
        // iterate through the old and new indices, if we're replacing a block with a block, update
        // the block. Otherwise, delete the old and insert the new
        let mut old_idx = old_index;
        let mut new_idx = new_index;
        while old_idx < old_index + old_len || new_idx < new_index + new_len {
            let old = if old_idx < old_index + old_len {
                self.old.get(old_idx)
            } else {
                None
            };
            let new = if new_idx < new_index + new_len {
                self.new.get(new_idx)
            } else {
                None
            };
            match (old, new) {
                (None, None) => {}
                (None, Some(val)) => match val {
                    BlockOrGrapheme::Block(b) => {
                        split_block(self.doc, self.tx, self.patch_log, self.obj, self.idx, b)?;
                        self.idx += 1;
                        new_idx += 1;
                    }
                    BlockOrGrapheme::Grapheme(g) => {
                        self.tx
                            .splice_text(self.doc, self.patch_log, self.obj, self.idx, 0, g)?;
                        self.idx += TextValue::width(g);
                        new_idx += 1;
                    }
                },
                (Some(val), None) => match val {
                    BlockOrGrapheme::Block(_) => {
                        self.tx
                            .join_block(self.doc, self.patch_log, self.obj, self.idx)?;
                        old_idx += 1;
                    }
                    BlockOrGrapheme::Grapheme(_) => {
                        self.tx
                            .delete(self.doc, self.patch_log, self.obj, self.idx)?;
                        old_idx += 1;
                    }
                },
                (Some(old), Some(new)) => match (old, new) {
                    (BlockOrGrapheme::Block(b1), BlockOrGrapheme::Block(b2)) => {
                        if b1 != b2 {
                            update_block(self.doc, self.tx, self.patch_log, self.obj, self.idx, b2)?
                        }
                        self.idx += 1;
                        old_idx += 1;
                        new_idx += 1;
                    }
                    (BlockOrGrapheme::Grapheme(_g1), BlockOrGrapheme::Grapheme(g2)) => {
                        self.tx
                            .delete(self.doc, self.patch_log, self.obj, self.idx)?;
                        self.tx
                            .splice_text(self.doc, self.patch_log, self.obj, self.idx, 0, g2)?;
                        self.idx += TextValue::width(g2);
                        old_idx += 1;
                        new_idx += 1;
                    }
                    (BlockOrGrapheme::Block(_), BlockOrGrapheme::Grapheme(g2)) => {
                        self.tx
                            .join_block(self.doc, self.patch_log, self.obj, self.idx)?;
                        self.tx
                            .splice_text(self.doc, self.patch_log, self.obj, self.idx, 0, g2)?;
                        self.idx += TextValue::width(g2);
                        old_idx += 1;
                        new_idx += 1;
                    }
                    (BlockOrGrapheme::Grapheme(_g1), BlockOrGrapheme::Block(b2)) => {
                        self.tx
                            .delete(self.doc, self.patch_log, self.obj, self.idx)?;
                        split_block(self.doc, self.tx, self.patch_log, self.obj, self.idx, b2)?;
                        self.idx += 1;
                        old_idx += 1;
                        new_idx += 1;
                    }
                },
            }
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn spans_as_grapheme(
    doc: &Automerge,
    text: &crate::types::ObjId,
    clock: Option<Clock>,
) -> Result<Vec<BlockOrGrapheme>, crate::AutomergeError> {
    let spans_internal = SpansInternal::new(
        OpTreeOpIter::new(doc.ops().iter_obj(text).unwrap(), doc.osd()),
        doc,
        clock.clone(),
    );
    let mut result = Vec::with_capacity(spans_internal.size_hint().0);
    for span in spans_internal {
        match span {
            SpanInternal::Obj(b, _) => {
                let crate::hydrate::Value::Map(map) = doc.hydrate_map(&b.into(), clock.as_ref())
                else {
                    tracing::warn!("unexpected non map object in text");
                    result.push(BlockOrGrapheme::Block(crate::hydrate::Map::new()));
                    continue;
                };
                result.push(BlockOrGrapheme::Block(map));
            }
            SpanInternal::Text(t, _, _) => {
                for g in t.graphemes(true) {
                    result.push(BlockOrGrapheme::Grapheme(g.to_string()));
                }
            }
        }
    }
    Ok(result)
}

fn block_or_text_as_grapheme<'a, I: Iterator<Item = BlockOrText<'a>>>(
    iter: I,
) -> Vec<BlockOrGrapheme> {
    let mut result = Vec::with_capacity(iter.size_hint().0);
    for b in iter {
        match b {
            BlockOrText::Block(b) => result.push(BlockOrGrapheme::Block(b)),
            BlockOrText::Text(t) => {
                for g in t.graphemes(true) {
                    result.push(BlockOrGrapheme::Grapheme(g.to_string()));
                }
            }
        }
    }
    result
}

fn split_block(
    doc: &mut Automerge,
    tx: &mut TransactionInner,
    patch_log: &mut PatchLog,
    obj: &crate::ObjId,
    index: usize,
    block: &crate::hydrate::Map,
) -> Result<(), crate::error::AutomergeError> {
    let new_block = tx.split_block(doc, patch_log, obj, index)?;
    tx.update_map(doc, patch_log, &new_block, block)
}

fn update_block(
    doc: &mut Automerge,
    tx: &mut TransactionInner,
    patch_log: &mut PatchLog,
    obj: &crate::ObjId,
    index: usize,
    new_block: &crate::hydrate::Map,
) -> Result<(), crate::error::AutomergeError> {
    let Some((crate::Value::Object(crate::ObjType::Map), block_id)) = doc.get(obj, index)? else {
        return Err(crate::error::AutomergeError::InvalidIndex(index));
    };

    tx.update_map(doc, patch_log, &block_id, new_block)
}
