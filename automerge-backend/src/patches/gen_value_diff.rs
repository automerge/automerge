use automerge_protocol as amp;

use super::PatchWorkshop;
use crate::op_handle::OpHandle;

pub(super) fn gen_value_diff(
    op: &OpHandle,
    value: &amp::ScalarValue,
    workshop: &dyn PatchWorkshop,
) -> amp::Diff {
    match value {
        amp::ScalarValue::Cursor(oid) => {
            // .expect() is okay here because we check that the cursr exists at the start of
            // `OpSet::apply_op()`
            amp::Diff::Cursor(workshop.find_cursor(oid).expect("missing cursor"))
        }
        _ => op.adjusted_value().into(),
    }
}
