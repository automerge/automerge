use std::cmp::Ordering;
use std::iter::Sum;
use std::num::NonZeroU32;
use std::ops::{Add, AddAssign, Div, Mul, Sub, SubAssign};

use std::fmt;

// Aggregate Accumulator
#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd)]
pub struct Acc(pub u64);

impl Acc {
    pub fn new() -> Self {
        Acc(0)
    }

    pub fn as_usize(&self) -> usize {
        self.0 as usize
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl Sub for Agg {
    type Output = Agg;
    fn sub(self, other: Self) -> Agg {
        match (self.0, other.0) {
            (Some(a), Some(b)) => Agg::from(a.get() - b.get()),
            _ => Agg(None),
        }
    }
}

impl Sub for Acc {
    type Output = Acc;
    fn sub(self, other: Self) -> Acc {
        Acc(self.0 - other.0)
    }
}

impl Sub<usize> for Acc {
    type Output = Acc;
    fn sub(self, other: usize) -> Acc {
        Acc(self.0 - other as u64)
    }
}

impl Add for Acc {
    type Output = Acc;
    fn add(self, other: Self) -> Acc {
        Acc(self.0 + other.0)
    }
}

impl Add<usize> for Acc {
    type Output = Acc;
    fn add(self, other: usize) -> Acc {
        Acc(self.0 + other as u64)
    }
}

impl Add<Agg> for Acc {
    type Output = Acc;
    fn add(self, other: Agg) -> Acc {
        Acc(self.0 + other.as_u64())
    }
}

impl AddAssign for Acc {
    fn add_assign(&mut self, other: Acc) {
        self.0 += other.0
    }
}

impl AddAssign<Agg> for Acc {
    fn add_assign(&mut self, other: Agg) {
        self.0 += other.as_u64()
    }
}

impl fmt::Display for Acc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}

impl SubAssign for Acc {
    fn sub_assign(&mut self, other: Acc) {
        self.0 = self.0.saturating_sub(other.0)
    }
}

impl SubAssign<usize> for Acc {
    fn sub_assign(&mut self, other: usize) {
        self.0 = self.0.saturating_sub(other as u64)
    }
}

impl Mul<usize> for Agg {
    type Output = Acc;
    fn mul(self, other: usize) -> Acc {
        Acc(self.as_u64() * (other as u64))
    }
}

impl Sum<Agg> for Acc {
    fn sum<I: Iterator<Item = Agg>>(iter: I) -> Self {
        Acc(iter.map(|a| a.as_u64()).sum::<u64>())
    }
}

impl Sum for Acc {
    fn sum<I: Iterator<Item = Acc>>(iter: I) -> Self {
        Acc(iter.map(|a| a.0).sum::<u64>())
    }
}

impl Div<Agg> for Acc {
    type Output = usize;
    fn div(self, other: Agg) -> usize {
        self.as_usize() / other.as_usize()
    }
}

impl From<usize> for Acc {
    fn from(v: usize) -> Self {
        Acc(v as u64)
    }
}

impl From<i32> for Acc {
    fn from(v: i32) -> Self {
        Self(v.try_into().unwrap_or(0))
    }
}

impl From<usize> for Agg {
    fn from(v: usize) -> Self {
        Self(u32::try_from(v).ok().and_then(NonZeroU32::new))
    }
}

// FIXME - panic for negative to i64?  Should never happen
impl From<i64> for Agg {
    fn from(v: i64) -> Self {
        Self(u32::try_from(v).ok().and_then(NonZeroU32::new))
    }
}

impl From<i32> for Agg {
    fn from(v: i32) -> Self {
        Self(u32::try_from(v).ok().and_then(NonZeroU32::new))
    }
}

impl From<u32> for Agg {
    fn from(v: u32) -> Self {
        Self(NonZeroU32::new(v))
    }
}

impl From<u64> for Agg {
    fn from(v: u64) -> Self {
        Self(u32::try_from(v).ok().and_then(NonZeroU32::new))
    }
}

impl From<u64> for Acc {
    fn from(v: u64) -> Self {
        Acc(v)
    }
}

impl PartialEq<usize> for Acc {
    fn eq(&self, other: &usize) -> bool {
        self.as_usize().eq(other)
    }
}

// Aggregate
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct Agg(Option<NonZeroU32>);

impl PartialOrd for Agg {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (&self.0, &other.0) {
            (None, None) => Some(Ordering::Equal),
            (Some(a), Some(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl Agg {
    pub fn new(v: u32) -> Self {
        Self(NonZeroU32::new(v))
    }

    pub fn iter(&self) -> impl Iterator<Item = Agg> {
        self.0.into_iter().map(|v| Agg(Some(v)))
    }

    pub fn is_some(&self) -> bool {
        self.0.is_some()
    }

    pub fn is_none(&self) -> bool {
        self.0.is_none()
    }

    pub fn maximize(&self, other: Self) -> Self {
        match (self.0, other.0) {
            (None, _) => other,
            (Some(a), Some(b)) if b > a => other,
            _ => *self,
        }
    }

    pub fn maximize_assign(&mut self, other: Self) {
        match (self.0, other.0) {
            (None, _) => {
                *self = other;
            }
            (Some(a), Some(b)) if b > a => {
                *self = other;
            }
            _ => (),
        }
    }

    pub fn minimize(&self, other: Self) -> Self {
        match (self.0, other.0) {
            (None, _) => other,
            (Some(a), Some(b)) if b < a => other,
            _ => *self,
        }
    }

    // FIXME - shouldnt this be option<usize>
    pub fn as_usize(&self) -> usize {
        self.0.map(|v| v.get() as usize).unwrap_or(0)
    }

    // FIXME - shouldnt this be option<u64>
    pub fn as_u64(&self) -> u64 {
        self.0.map(|v| v.get() as u64).unwrap_or(0)
    }

    pub fn as_i64(&self) -> Option<i64> {
        self.0.map(|v| v.get() as i64)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn agg_partial_cmp() {
        let agg0 = Agg::from(0);
        let agg1 = Agg::from(1);
        let agg2 = Agg::from(2);
        assert!(agg0 == agg0);
        assert!(agg2 == agg2);
        assert!(agg1 < agg2);
        assert!(agg0.partial_cmp(&agg1).is_none());
        assert!(agg0.partial_cmp(&agg1).is_none());
        assert!(agg1.partial_cmp(&agg2) == Some(Ordering::Less));
    }
}
