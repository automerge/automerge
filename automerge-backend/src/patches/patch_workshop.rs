use automerge_protocol as amp;
use smol_str::SmolStr;

use crate::{
    internal::{Key, ObjectId, OpId},
    object_store::ObjState,
};

/// An abstraction over the information `PendingDiffs` needs access to in order
/// to generate a `Patch`. In practice the implementation will always be an
/// `OpSet` but this abstraction boundary makes it easier to avoid accidentally
/// coupling the patch generation to internals of the `OpSet`
///
/// It's a "workshop" because it's not a factory, it doesn't do the actual
/// building of the patch. It's just where some tools to make the patch can be
/// found
pub(crate) trait PatchWorkshop {
    fn key_to_string(&self, key: &Key) -> SmolStr;
    fn find_cursor(&self, opid: &amp::OpId) -> Option<amp::CursorDiff>;
    fn get_obj(&self, object_id: &ObjectId) -> Option<&ObjState>;
    fn make_external_objid(&self, object_id: &ObjectId) -> amp::ObjectId;
    fn make_external_opid(&self, opid: &OpId) -> amp::OpId;
}
