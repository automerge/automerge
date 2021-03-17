use super::StateTreeChange;

pub(super) struct DiffApplicationResult<T> {
    pub(super) value: T,
    pub(super) change: StateTreeChange,
}

impl<T> DiffApplicationResult<T> {
    pub(crate) fn pure(t: T) -> DiffApplicationResult<T> {
        DiffApplicationResult {
            value: t,
            change: StateTreeChange::empty(),
        }
    }

    pub(crate) fn with_changes(mut self, changes: StateTreeChange) -> Self {
        self.change = changes;
        self
    }

    pub(crate) fn map<F, U>(self, f: F) -> DiffApplicationResult<U>
    where
        F: FnOnce(T) -> U,
    {
        DiffApplicationResult {
            value: f(self.value),
            change: self.change,
        }
    }

    pub(crate) fn try_map<F, U, E>(self, f: F) -> Result<DiffApplicationResult<U>, E>
    where
        F: FnOnce(T) -> Result<U, E>,
    {
        let value = f(self.value)?;
        Ok(DiffApplicationResult {
            value,
            change: self.change,
        })
    }

    pub(crate) fn and_then<F, U>(self, f: F) -> DiffApplicationResult<U>
    where
        F: FnOnce(T) -> DiffApplicationResult<U>,
    {
        let result = f(self.value);
        DiffApplicationResult {
            value: result.value,
            change: result.change.union(self.change),
        }
    }

    pub(crate) fn try_and_then<F, U, E>(self, f: F) -> Result<DiffApplicationResult<U>, E>
    where
        F: FnOnce(T) -> Result<DiffApplicationResult<U>, E>,
    {
        let result = f(self.value)?;
        Ok(DiffApplicationResult {
            value: result.value,
            change: result.change.union(self.change),
        })
    }
}
