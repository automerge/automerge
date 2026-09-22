use super::*;

use crate::types::ScalarValue;

fn id(counter: u64) -> OpId {
    OpId::new(counter, 0)
}

fn counter(n: i64) -> Value {
    Value::scalar(ScalarValue::counter(n))
}

fn existing(candidates: &[(u64, Value)]) -> CandidateSummary {
    let mut summary = CandidateSummary::default();
    for (counter, value) in candidates {
        summary.add_existing(id(*counter), value.clone());
    }
    summary
}

fn present(summary: &CandidateSummary) -> &PresentCandidates {
    let Summary::Present(present) = &summary.0 else {
        panic!("expected a present summary, got {summary:?}");
    };
    present
}

mod bounded_summary;
mod patch_emission;
mod retained_encoding;
mod summary_accumulation;
mod value_transition;
