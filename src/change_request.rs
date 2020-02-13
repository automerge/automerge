use crate::value::Value;

/// Represents the various changes that you can make to a document, all of
/// these use a "path" to refer to parts of the document. You can generate
/// paths using a builder syntax. E.g this would refer to the second element
/// of an array under the "people" key in the root object
///
/// ```rust,no_run
/// # use automerge::{Path, ListIndex};
/// Path::root().key("people".to_string()).index(ListIndex::Index(1));
/// ```
///
/// Note that there is a special `ListIndex` for the head of a list, in case
/// you want to insert something at the beginning
#[derive(Debug)]
pub enum ChangeRequest {
    Set { path: Path, value: Value },
    Move { from: Path, to: Path },
    Delete { path: Path },
    Increment { path: Path, value: f64 },
    InsertAfter { path: Path, value: Value },
}

#[derive(Clone, Debug, PartialEq)]
pub enum PathElement {
    Root,
    Key(String),
    Index(ListIndex),
}

#[derive(Clone, Debug, PartialEq)]
pub enum ListIndex {
    Head,
    Index(usize),
}

/// Represents a location within a document
#[derive(Debug)]
pub struct Path(Vec<PathElement>);

impl Path {

    /// A path at the root of the document
    pub fn root() -> Path {
        Path(vec![PathElement::Root])
    }

    /// Returns a new path which points to the list element at index of the
    /// current path
    pub fn index(&self, index: ListIndex) -> Path {
        let mut elems = self.0.clone();
        elems.push(PathElement::Index(index));
        Path(elems)
    }

    /// Returns a new path which points to the element under this key in the 
    /// current path
    pub fn key(&self, key: String) -> Path {
        let mut elems = self.0.clone();
        elems.push(PathElement::Key(key));
        Path(elems)
    }

    /// Returns the parent of this part
    pub fn parent(&self) -> Path {
        Path(self.0.clone().into_iter().skip(1).collect())
    }

    pub fn is_root(&self) -> bool {
        self.0.len() == 1 && self.0[0] == PathElement::Root
    }
}

impl<'a> IntoIterator for &'a Path {
    type Item = &'a PathElement;
    type IntoIter = std::slice::Iter<'a, PathElement>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}
