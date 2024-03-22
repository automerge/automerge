use unicode_segmentation::UnicodeSegmentation;

use crate::{
    text_value::TextValue, transaction::TransactionInner, Automerge, ObjId as ExId, PatchLog,
    ReadDoc,
};
mod myers;
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
