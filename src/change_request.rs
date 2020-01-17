use crate::value::Value;

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

#[derive(Debug)]
pub struct Path(Vec<PathElement>);

impl Path {
    pub fn root() -> Path {
        Path(vec![PathElement::Root])
    }

    pub fn index(&self, index: ListIndex) -> Path {
        let mut elems = self.0.clone();
        elems.push(PathElement::Index(index));
        Path(elems)
    }

    pub fn key(&self, key: String) -> Path {
        let mut elems = self.0.clone();
        elems.push(PathElement::Key(key));
        Path(elems)
    }

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
