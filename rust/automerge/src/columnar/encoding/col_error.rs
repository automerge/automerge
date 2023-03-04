#[derive(Clone, Debug)]
pub struct DecodeColumnError {
    path: Path,
    error: DecodeColErrorKind,
}

impl std::error::Error for DecodeColumnError {}

impl std::fmt::Display for DecodeColumnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.error {
            DecodeColErrorKind::UnexpectedNull => {
                write!(f, "unexpected null in column {}", self.path)
            }
            DecodeColErrorKind::InvalidValue { reason } => {
                write!(f, "invalid value in column {}: {}", self.path, reason)
            }
        }
    }
}

#[derive(Clone, Debug)]
struct Path(Vec<String>);

impl std::fmt::Display for Path {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, elem) in self.0.iter().rev().enumerate() {
            if index != 0 {
                write!(f, ":")?;
            }
            write!(f, "{}", elem)?;
        }
        Ok(())
    }
}

impl Path {
    fn push<S: AsRef<str>>(&mut self, col: S) {
        self.0.push(col.as_ref().to_string())
    }
}

impl<S: AsRef<str>> From<S> for Path {
    fn from(p: S) -> Self {
        Self(vec![p.as_ref().to_string()])
    }
}

#[derive(Clone, Debug)]
enum DecodeColErrorKind {
    UnexpectedNull,
    InvalidValue { reason: String },
}

impl DecodeColumnError {
    pub(crate) fn decode_raw<S: AsRef<str>>(col: S, raw_err: super::raw::Error) -> Self {
        Self {
            path: col.into(),
            error: DecodeColErrorKind::InvalidValue {
                reason: raw_err.to_string(),
            },
        }
    }

    pub(crate) fn unexpected_null<S: AsRef<str>>(col: S) -> DecodeColumnError {
        Self {
            path: col.into(),
            error: DecodeColErrorKind::UnexpectedNull,
        }
    }

    pub(crate) fn invalid_value<S: AsRef<str>, R: AsRef<str>>(
        col: S,
        reason: R,
    ) -> DecodeColumnError {
        Self {
            path: col.into(),
            error: DecodeColErrorKind::InvalidValue {
                reason: reason.as_ref().to_string(),
            },
        }
    }

    pub(crate) fn in_column<S: AsRef<str>>(mut self, col: S) -> DecodeColumnError {
        self.path.push(col.as_ref());
        self
    }
}
