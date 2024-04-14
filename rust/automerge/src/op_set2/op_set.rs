use crate::storage::{columns::compression, ColumnSpec, Document, RawColumn, RawColumns};
use crate::types::ActorId;

use super::{Column, Slab};

use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Default, Clone)]
pub(crate) struct OpSet {
    len: usize,
    actors: Vec<ActorId>,
    cols: BTreeMap<ColumnSpec, Column>,
}

impl OpSet {
    pub(crate) fn new(doc: &Document<'_>) -> Self {
        // FIXME - shouldn't need to clone bytes here (eventually)
        let data = Arc::new(doc.op_raw_bytes().to_vec());
        let actors = doc.actors().to_vec();
        let cols = doc
            .op_metadata
            .raw_columns()
            .iter()
            .map(|c| {
                (
                    c.spec(),
                    Column::external(c.spec(), data.clone(), c.data()).unwrap(),
                )
            })
            .collect();
        let mut op_set = OpSet {
            actors,
            cols,
            len: 0,
        };
        op_set.len = op_set
            .cols
            .first_key_value()
            .map(|(_, c)| c.len())
            .unwrap_or(0);
        op_set
    }

    fn export(&self) -> (RawColumns<compression::Uncompressed>, Vec<u8>) {
        let mut data = vec![]; // should be able to do with_capacity here
        let mut raw = vec![];
        for (spec, c) in &self.cols {
            let range = c.write(&mut data);
            if !range.is_empty() {
                raw.push(RawColumn::new(*spec, range));
            }
        }
        (raw.into_iter().collect(), data)
    }

    fn iter(&self) -> OpIter {
        OpIter { index: 0 }
    }

    // iter ops

    // better error handling
    // export bytes
    // insert op
    // seek nth (read)
    // seek nth (insert)
    // seek prop
    // seek opid
    // seek mark

    // split slabs at some point

    // slab in-place edits
    // slab index vec<cursor>

    // ugly api stuff
    //
    // * boolean packable has unused pack/unpack - maybe we want two traits
    //    one for Rle<> and one for Cursor<> that overlap?
    // * columns that don't handle nulls still take Option<Item> and the
    //    iterator still returns Option<item> - could be nice to more cleanly
    //    handle columns that can't take nulls - currently hide this with
    //    MaybePackable allowing you to pass in Item or Option<Item> to splice
    // * maybe do something with types to make scan required to get
    //    validated bytes
}

struct OpIter {
    index: usize,
}

struct Op {}
