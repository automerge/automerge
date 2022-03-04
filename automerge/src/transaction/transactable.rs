use crate::exid::ExId;
use crate::types::{ObjId, OpId};
use crate::{
    Automerge, AutomergeError, ChangeHash, Keys, KeysAt, ObjType, OpType, Prop, ScalarValue, Value,
};
use unicode_segmentation::UnicodeSegmentation;

mod private {
    use crate::{ObjType, ScalarValue};

    pub trait Sealed {}

    impl Sealed for ScalarValue {}
    impl Sealed for ObjType {}
    impl Sealed for i64 {}
    impl Sealed for () {}
}

pub trait CanSet: private::Sealed {
    type Source;
    type Result;

    fn construct_result(doc: &Automerge, opid: OpId) -> Self::Result;

    fn into_optype(self) -> OpType;
}

impl CanSet for ScalarValue {
    type Source = Self;
    type Result = ();

    fn construct_result(_: &Automerge, _: OpId) -> Self::Result {}

    fn into_optype(self) -> OpType {
        OpType::Set(self)
    }
}

impl CanSet for ObjType {
    type Source = Self;
    type Result = ExId;

    fn construct_result(doc: &Automerge, opid: OpId) -> Self::Result {
        doc.id_to_exid(opid)
    }

    fn into_optype(self) -> OpType {
        OpType::Make(self)
    }
}

impl CanSet for i64 {
    type Source = Self;
    type Result = ();

    fn construct_result(_: &Automerge, _: OpId) -> Self::Result {}

    fn into_optype(self) -> OpType {
        OpType::Inc(self)
    }
}

impl CanSet for () {
    type Source = Self;
    type Result = ();

    fn construct_result(_: &Automerge, _: OpId) -> Self::Result {}

    fn into_optype(self) -> OpType {
        OpType::Del
    }
}

/// A way of mutating a document within a single change.
pub trait Transactable {
    /// Get the number of pending operations in this transaction.
    fn pending_ops(&self) -> usize;

    /// Set the value of property `P` to value `V` in object `obj`.
    ///
    /// # Errors
    ///
    /// This will return an error if
    /// - The object does not exist
    /// - The key is the wrong type for the object
    /// - The key does not exist in the object
    fn set<O: AsRef<ExId>, P: Into<Prop>, C: CanSet, V: Into<C>>(
        &mut self,
        obj: O,
        prop: P,
        value: V,
    ) -> Result<C::Result, AutomergeError>;

    /// Insert a value into a list at the given index.
    fn insert<O: AsRef<ExId>, V: CanSet>(
        &mut self,
        obj: O,
        index: usize,
        value: V,
    ) -> Result<V::Result, AutomergeError>;

    /// Increment the counter at the prop in the object by `value`.
    fn inc<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError>;

    /// Delete the value at prop in the object.
    fn del<O: AsRef<ExId>, P: Into<Prop>>(&mut self, obj: O, prop: P)
        -> Result<(), AutomergeError>;

    fn splice<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        pos: usize,
        del: usize,
        vals: Vec<ScalarValue>,
    ) -> Result<(), AutomergeError>;

    /// Like [`Self::splice`] but for text.
    fn splice_text<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        pos: usize,
        del: usize,
        text: &str,
    ) -> Result<(), AutomergeError> {
        let mut vals = vec![];
        for c in text.to_owned().graphemes(true) {
            vals.push(c.into());
        }
        self.splice(obj, pos, del, vals)
    }

    /// Get the keys of the given object, it should be a map.
    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys;

    /// Get the keys of the given object at a point in history.
    fn keys_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> KeysAt;

    /// Get the length of the given object.
    fn length<O: AsRef<ExId>>(&self, obj: O) -> usize;

    /// Get the length of the given object at a point in history.
    fn length_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> usize;

    /// Get the string that this text object represents.
    fn text<O: AsRef<ExId>>(&self, obj: O) -> Result<String, AutomergeError>;

    /// Get the string that this text object represents at a point in history.
    fn text_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<String, AutomergeError>;

    /// Get the value at this prop in the object.
    fn value<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value, ExId)>, AutomergeError>;

    /// Get the value at this prop in the object at a point in history.
    fn value_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value, ExId)>, AutomergeError>;

    fn values<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value, ExId)>, AutomergeError>;

    fn values_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value, ExId)>, AutomergeError>;
}
