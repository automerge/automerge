// This file was copied from the similar crate at
// https://github.com/mitsuhiko/similar/blob/2b31f65445df9093ba007ca5a5ae6a71b899d491/src/algorithms/replace.rs
// The original license is in the LICENSE file in the same directory as this file
//
// This file was modified to use a Diff trait defined in this crate rather than the DiffHook trait
use super::myers::DiffHook;

/// A [`DiffHook`] that combines deletions and insertions to give blocks
/// of maximal length, and replacements when appropriate.
///
/// It will replace [`DiffHook::insert`] and [`DiffHook::delete`] events when
/// possible with [`DiffHook::replace`] events.  Note that even though the
/// text processing in the crate does not use replace events and always resolves
/// then back to delete and insert, it's useful to always use the replacer to
/// ensure a consistent order of inserts and deletes.  This is why for instance
/// the text diffing automatically uses this hook internally.
pub(super) struct Replace<D: DiffHook> {
    d: D,
    del: Option<(usize, usize, usize)>,
    ins: Option<(usize, usize, usize)>,
    eq: Option<(usize, usize, usize)>,
}

impl<D: DiffHook> Replace<D> {
    /// Creates a new replace hook wrapping another hook.
    pub(super) fn new(d: D) -> Self {
        Replace {
            d,
            del: None,
            ins: None,
            eq: None,
        }
    }

    /// Extracts the inner hook.
    #[allow(dead_code)]
    pub(super) fn into_inner(self) -> D {
        self.d
    }

    fn flush_eq(&mut self) -> Result<(), D::Error> {
        if let Some((eq_old_index, eq_new_index, eq_len)) = self.eq.take() {
            self.d.equal(eq_old_index, eq_new_index, eq_len)?
        }
        Ok(())
    }

    fn flush_del_ins(&mut self) -> Result<(), D::Error> {
        if let Some((del_old_index, del_old_len, del_new_index)) = self.del.take() {
            if let Some((_, ins_new_index, ins_new_len)) = self.ins.take() {
                self.d
                    .replace(del_old_index, del_old_len, ins_new_index, ins_new_len)?;
            } else {
                self.d.delete(del_old_index, del_old_len, del_new_index)?;
            }
        } else if let Some((ins_old_index, ins_new_index, ins_new_len)) = self.ins.take() {
            self.d.insert(ins_old_index, ins_new_index, ins_new_len)?;
        }
        Ok(())
    }
}

impl<D: DiffHook> AsRef<D> for Replace<D> {
    fn as_ref(&self) -> &D {
        &self.d
    }
}

impl<D: DiffHook> AsMut<D> for Replace<D> {
    fn as_mut(&mut self) -> &mut D {
        &mut self.d
    }
}

impl<D: DiffHook> DiffHook for Replace<D> {
    type Error = D::Error;

    fn equal(&mut self, old_index: usize, new_index: usize, len: usize) -> Result<(), D::Error> {
        self.flush_del_ins()?;

        self.eq = if let Some((eq_old_index, eq_new_index, eq_len)) = self.eq.take() {
            Some((eq_old_index, eq_new_index, eq_len + len))
        } else {
            Some((old_index, new_index, len))
        };

        Ok(())
    }

    fn delete(
        &mut self,
        old_index: usize,
        old_len: usize,
        new_index: usize,
    ) -> Result<(), D::Error> {
        self.flush_eq()?;
        if let Some((del_old_index, del_old_len, del_new_index)) = self.del.take() {
            debug_assert_eq!(old_index, del_old_index + del_old_len);
            self.del = Some((del_old_index, del_old_len + old_len, del_new_index));
        } else {
            self.del = Some((old_index, old_len, new_index));
        }
        Ok(())
    }

    fn insert(
        &mut self,
        old_index: usize,
        new_index: usize,
        new_len: usize,
    ) -> Result<(), D::Error> {
        self.flush_eq()?;
        self.ins = if let Some((ins_old_index, ins_new_index, ins_new_len)) = self.ins.take() {
            debug_assert_eq!(ins_new_index + ins_new_len, new_index);
            Some((ins_old_index, ins_new_index, new_len + ins_new_len))
        } else {
            Some((old_index, new_index, new_len))
        };

        Ok(())
    }

    fn replace(
        &mut self,
        old_index: usize,
        old_len: usize,
        new_index: usize,
        new_len: usize,
    ) -> Result<(), D::Error> {
        self.flush_eq()?;
        self.d.replace(old_index, old_len, new_index, new_len)
    }

    fn finish(&mut self) -> Result<(), D::Error> {
        self.flush_eq()?;
        self.flush_del_ins()?;
        self.d.finish()
    }
}
