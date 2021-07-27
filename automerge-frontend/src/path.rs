use std::fmt;

use smol_str::SmolStr;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum PathElement {
    Key(SmolStr),
    Index(u32),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Path(Vec<PathElement>);

impl Path {
    pub(crate) fn has_prefix(&self, prefix: &Path) -> bool {
        if self.0.len() < prefix.0.len() {
            return false;
        }

        // takes the shorter of the two which should be the prefix
        self.0
            .iter()
            .zip(prefix.0.iter())
            .all(|(ours, theirs)| ours == theirs)
    }

    pub fn root() -> Path {
        Path(Vec::new())
    }

    pub fn index(mut self, index: u32) -> Self {
        self.0.push(PathElement::Index(index));
        self
    }

    pub fn key<S: Into<SmolStr>>(mut self, key: S) -> Path {
        self.0.push(PathElement::Key(key.into()));
        self
    }

    pub fn parent(&self) -> Self {
        if self.0.is_empty() {
            Path(Vec::new())
        } else {
            let mut new_path = self.0.clone();
            new_path.pop();
            Path(new_path)
        }
    }

    /// Get the final component of the path, if any
    pub(crate) fn name(&self) -> Option<&PathElement> {
        self.0.last()
    }

    pub(crate) fn elements(self) -> Vec<PathElement> {
        self.0
    }

    pub(crate) fn is_root(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Display for PathElement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PathElement::Key(k) => write!(f, "{}", k),
            PathElement::Index(i) => write!(f, "{}", i),
        }
    }
}
