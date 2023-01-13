use super::{DeltaRange, RleRange};
use crate::columnar::encoding::{DecodeColumnError, DeltaDecoder, RleDecoder};

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

impl<'a> DepsIter<'a> {
    fn try_next(&mut self) -> Result<Option<Vec<u64>>, DecodeColumnError> {
        let num = match self
            .num
            .next()
            .transpose()
            .map_err(|e| DecodeColumnError::decode_raw("num", e))?
        {
            Some(Some(n)) => n as usize,
            Some(None) => {
                return Err(DecodeColumnError::unexpected_null("group"));
            }
            None => return Ok(None),
        };
        // We cannot trust `num` because it is provided over the network,
        // but in the common case it will be correct and small (so we
        // use with_capacity to make sure the vector is precisely the right
        // size).
        let mut result = Vec::with_capacity(std::cmp::min(num, 100));
        while result.len() < num {
            match self
                .deps
                .next()
                .transpose()
                .map_err(|e| DecodeColumnError::decode_raw("deps", e))?
            {
                Some(Some(elem)) => {
                    let elem = match u64::try_from(elem) {
                        Ok(e) => e,
                        Err(e) => {
                            tracing::error!(err=?e, dep=elem, "error converting dep index to u64");
                            return Err(DecodeColumnError::invalid_value(
                                "deps",
                                "error converting dep index to u64",
                            ));
                        }
                    };
                    result.push(elem);
                }
                _ => return Err(DecodeColumnError::unexpected_null("deps")),
            }
        }
        Ok(Some(result))
    }
}

impl<'a> Iterator for DepsIter<'a> {
    type Item = Result<Vec<u64>, DecodeColumnError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
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
