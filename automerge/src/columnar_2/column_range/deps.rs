use super::{DeltaRange, RleRange};
use crate::columnar_2::encoding::{DecodeColumnError, DeltaDecoder, RleDecoder};

/// A grouped column containing lists of u64s
#[derive(Clone, Debug)]
pub(crate) struct DepsRange {
    num: RleRange<u64>,
    deps: DeltaRange,
}

impl DepsRange {
    pub(crate) fn new(num: RleRange<u64>, deps: DeltaRange) -> Self {
        Self { num, deps }
    }

    pub(crate) fn num_range(&self) -> &RleRange<u64> {
        &self.num
    }

    pub(crate) fn deps_range(&self) -> &DeltaRange {
        &self.deps
    }

    pub(crate) fn encode<I, II>(deps: I, out: &mut Vec<u8>) -> DepsRange
    where
        I: Iterator<Item = II> + Clone,
        II: IntoIterator<Item = u64> + ExactSizeIterator,
    {
        let num = RleRange::encode(deps.clone().map(|d| Some(d.len() as u64)), out);
        let deps = DeltaRange::encode(
            deps.flat_map(|d| d.into_iter().map(|d| Some(d as i64))),
            out,
        );
        DepsRange { num, deps }
    }

    pub(crate) fn iter<'a>(&self, data: &'a [u8]) -> DepsIter<'a> {
        DepsIter {
            num: self.num.decoder(data),
            deps: self.deps.decoder(data),
        }
    }
}

#[derive(Clone)]
pub(crate) struct DepsIter<'a> {
    num: RleDecoder<'a, u64>,
    deps: DeltaDecoder<'a>,
}

impl<'a> Iterator for DepsIter<'a> {
    type Item = Result<Vec<u64>, DecodeColumnError>;
    fn next(&mut self) -> Option<Self::Item> {
        let num = match self.num.next() {
            Some(Some(n)) => n as usize,
            Some(None) => {
                return Some(Err(DecodeColumnError::UnexpectedNull(
                    "deps group".to_string(),
                )))
            }
            None => return None,
        };
        let mut result = Vec::with_capacity(num);
        while result.len() < num {
            match self.deps.next() {
                Some(Some(elem)) => {
                    let elem = match u64::try_from(elem) {
                        Ok(e) => e,
                        Err(e) => {
                            tracing::error!(err=?e, dep=elem, "error converting dep index to u64");
                            return Some(Err(DecodeColumnError::InvalidValue {
                                column: "deps".to_string(),
                                description: "error converting dep index to u64".to_string(),
                            }));
                        }
                    };
                    result.push(elem);
                }
                _ => return Some(Err(DecodeColumnError::UnexpectedNull("deps".to_string()))),
            }
        }
        Some(Ok(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::collection::vec as propvec;
    use proptest::prelude::*;

    fn encodable_u64() -> impl Strategy<Value = u64> + Clone {
        0_u64..((i64::MAX / 2) as u64)
    }

    proptest! {
        #[test]
        fn encode_decode_deps(deps in propvec(propvec(encodable_u64(), 0..100), 0..100)) {
            let mut out = Vec::new();
            let range = DepsRange::encode(deps.iter().cloned().map(|d| d.into_iter()), &mut out);
            let decoded = range.iter(&out).collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(deps, decoded);
        }
    }
}
