//! Helpers for property tests.

use std::{fmt::Debug, ops::Range};

use proptest::prelude::*;
use smol_str::SmolStr;

use crate::{
    columnar::Key,
    types::{ElemId, OpId, ScalarValue},
};

#[derive(Clone, Debug)]
pub(crate) struct SpliceScenario<T> {
    pub(crate) initial_values: Vec<T>,
    pub(crate) replace_range: Range<usize>,
    pub(crate) replacements: Vec<T>,
}

impl<T: Debug + PartialEq + Clone> SpliceScenario<T> {
    pub(crate) fn check(&self, results: Vec<T>) {
        let mut expected = self.initial_values.clone();
        expected.splice(self.replace_range.clone(), self.replacements.clone());
        assert_eq!(expected, results)
    }
}

impl<T: Debug + PartialEq + Clone> SpliceScenario<Option<T>> {
    /// Checks that `results` are the same as `SpliceScenario::initial_values.splice(replace_range,
    /// replacements)`, with two slight changes:
    ///
    /// * If all of `initial_values` are `None` then this returns true if the output is just
    ///   `replacements`
    /// * If the result of `Vec::splice` would return a vector of all `None` then this checks the
    ///   result is actually an empty vector
    ///
    /// This is to accomodate the fact that the RLE encoder can encode a sequence of all `None` as
    /// an empty sequence, in which case we decode it as an empty sequence.
    pub(crate) fn check_optional(&self, results: Vec<Option<T>>) {
        if self.initial_values.iter().all(|v| v.is_none()) {
            if self.replacements.iter().all(|v| v.is_none()) {
                assert!(results.is_empty());
            } else {
                assert_eq!(results, self.replacements);
            }
        } else {
            let mut expected = self.initial_values.clone();
            expected.splice(self.replace_range.clone(), self.replacements.clone());
            if expected.iter().all(|e| e.is_none()) {
                assert!(results.is_empty())
            } else {
                assert_eq!(expected, results)
            }
        }
    }
}

pub(crate) fn splice_scenario<S: Strategy<Value = T> + Clone, T: Debug + Clone + 'static>(
    item_strat: S,
) -> impl Strategy<Value = SpliceScenario<T>> {
    (
        proptest::collection::vec(item_strat.clone(), 0..100),
        proptest::collection::vec(item_strat, 0..10),
    )
        .prop_flat_map(move |(values, to_splice)| {
            if values.is_empty() {
                Just(SpliceScenario {
                    initial_values: values,
                    replace_range: 0..0,
                    replacements: to_splice,
                })
                .boxed()
            } else {
                // This is somewhat awkward to write because we have to carry the `values` and
                // `to_splice` through as `Just(..)` to please the borrow checker.
                (0..values.len(), Just(values), Just(to_splice))
                    .prop_flat_map(move |(replace_range_start, values, to_splice)| {
                        (
                            0..(values.len() - replace_range_start),
                            Just(values),
                            Just(to_splice),
                        )
                            .prop_map(
                                move |(replace_range_len, values, to_splice)| SpliceScenario {
                                    initial_values: values,
                                    replace_range: replace_range_start
                                        ..(replace_range_start + replace_range_len),
                                    replacements: to_splice,
                                },
                            )
                    })
                    .boxed()
            }
        })
}

/// Like splice scenario except that if the initial values we generate are all `None` then the
/// replace range is 0..0.
pub(crate) fn option_splice_scenario<
    S: Strategy<Value = Option<T>> + Clone,
    T: Debug + Clone + 'static,
>(
    item_strat: S,
) -> impl Strategy<Value = SpliceScenario<Option<T>>> {
    (
        proptest::collection::vec(item_strat.clone(), 0..100),
        proptest::collection::vec(item_strat, 0..10),
    )
        .prop_flat_map(move |(values, to_splice)| {
            if values.is_empty() || values.iter().all(|v| v.is_none()) {
                Just(SpliceScenario {
                    initial_values: values,
                    replace_range: 0..0,
                    replacements: to_splice,
                })
                .boxed()
            } else {
                // This is somewhat awkward to write because we have to carry the `values` and
                // `to_splice` through as `Just(..)` to please the borrow checker.
                (0..values.len(), Just(values), Just(to_splice))
                    .prop_flat_map(move |(replace_range_start, values, to_splice)| {
                        (
                            0..(values.len() - replace_range_start),
                            Just(values),
                            Just(to_splice),
                        )
                            .prop_map(
                                move |(replace_range_len, values, to_splice)| SpliceScenario {
                                    initial_values: values,
                                    replace_range: replace_range_start
                                        ..(replace_range_start + replace_range_len),
                                    replacements: to_splice,
                                },
                            )
                    })
                    .boxed()
            }
        })
}

pub(crate) fn opid() -> impl Strategy<Value = OpId> + Clone {
    (0..(u32::MAX as usize), 0..(u32::MAX as u64)).prop_map(|(actor, ctr)| OpId::new(ctr, actor))
}

pub(crate) fn elemid() -> impl Strategy<Value = ElemId> + Clone {
    opid().prop_map(ElemId)
}

pub(crate) fn key() -> impl Strategy<Value = Key> + Clone {
    prop_oneof! {
        elemid().prop_map(Key::Elem),
        any::<String>().prop_map(|s| Key::Prop(s.into())),
    }
}

pub(crate) fn encodable_int() -> impl Strategy<Value = i64> + Clone {
    let bounds = i64::MAX / 2;
    -bounds..bounds
}

pub(crate) fn scalar_value() -> impl Strategy<Value = ScalarValue> + Clone {
    prop_oneof! {
        Just(ScalarValue::Null),
        any::<bool>().prop_map(ScalarValue::Boolean),
        any::<u64>().prop_map(ScalarValue::Uint),
        encodable_int().prop_map(ScalarValue::Int),
        any::<f64>().prop_map(ScalarValue::F64),
        smol_str().prop_map(ScalarValue::Str),
        any::<Vec<u8>>().prop_map(ScalarValue::Bytes),
        encodable_int().prop_map(|i| ScalarValue::Counter(i.into())),
        encodable_int().prop_map(ScalarValue::Timestamp),
        (10..15_u8, any::<Vec<u8>>()).prop_map(|(c, b)| ScalarValue::Unknown { type_code: c, bytes: b }),
    }
}

fn smol_str() -> impl Strategy<Value = SmolStr> + Clone {
    any::<String>().prop_map(SmolStr::from)
}
