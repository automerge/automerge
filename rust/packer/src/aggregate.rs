use std::iter::Sum;
use std::num::NonZeroU32;
use std::ops::{Add, AddAssign, Div, Mul, Sub, SubAssign};

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
        Acc(iter.map(|a| a.as_usize()).sum::<usize>() as u64)
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
#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd)]
pub struct Agg(Option<NonZeroU32>);

impl Agg {
    pub fn new(v: u32) -> Self {
        Self(NonZeroU32::new(v))
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

    pub fn as_usize(&self) -> usize {
        self.0.map(|v| v.get() as usize).unwrap_or(0)
    }

    pub fn as_u64(&self) -> u64 {
        self.0.map(|v| v.get() as u64).unwrap_or(0)
    }
}
