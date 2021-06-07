use std::mem;

use automerge_protocol as amp;

#[derive(Debug)]
pub(crate) struct Edits(Vec<amp::DiffEdit>);

impl Edits {
    pub(crate) fn new() -> Edits {
        Edits(Vec::new())
    }

    /// Append an edit to this sequence, collapsing it into the last edit if possible.
    ///
    /// The collapsing handles conversion of a sequence of inserts to a multi-insert.
    pub(crate) fn append_edit(&mut self, edit: amp::DiffEdit) {
        if let Some(mut last) = self.0.last_mut() {
            match (&mut last, edit) {
                (
                    amp::DiffEdit::SingleElementInsert {
                        index,
                        elem_id,
                        op_id,
                        value: amp::Diff::Value(value),
                    },
                    amp::DiffEdit::SingleElementInsert {
                        index: next_index,
                        elem_id: next_elem_id,
                        op_id: next_op_id,
                        value: amp::Diff::Value(next_value),
                    },
                ) if *index + 1 == next_index
                    && elem_id.as_opid() == Some(op_id)
                    && next_elem_id.as_opid() == Some(&next_op_id)
                    // Ensure the values have a common type
                    && std::mem::discriminant(value) == std::mem::discriminant(&next_value)
                    && op_id.delta(&next_op_id, 1) =>
                {
                    *last = amp::DiffEdit::MultiElementInsert {
                        index: *index,
                        elem_id: elem_id.clone(),
                        values: vec![mem::replace(value, amp::ScalarValue::Null), next_value],
                    };
                }
                (
                    amp::DiffEdit::MultiElementInsert {
                        index,
                        elem_id,
                        values,
                    },
                    amp::DiffEdit::SingleElementInsert {
                        index: next_index,
                        elem_id: next_elem_id,
                        op_id,
                        value: amp::Diff::Value(value),
                    },
                ) if *index + (values.len() as u64) == next_index
                    && next_elem_id.as_opid() == Some(&op_id)
                    // Ensure the values have a common type
                    && std::mem::discriminant(&values[0]) == std::mem::discriminant(&value)
                    && elem_id
                        .as_opid()
                        .unwrap()
                        .delta(&op_id, values.len() as u64) =>
                {
                    values.push(value);
                }
                (
                    amp::DiffEdit::Remove { index, count },
                    amp::DiffEdit::Remove {
                        index: new_index,
                        count: new_count,
                    },
                ) if *index == new_index => *count += new_count,
                (_, edit) => self.0.push(edit),
            }
        } else {
            self.0.push(edit)
        }
    }

    pub(crate) fn into_vec(self) -> Vec<amp::DiffEdit> {
        self.0
    }
}
