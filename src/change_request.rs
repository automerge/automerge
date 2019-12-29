#[derive(Debug)]
pub enum ChangeRequest {
    Set {
        path: Path,
        value: serde_json::Value,
    },
    Move {
        from: Path,
        to: Path,
    },
    Delete {
        path: Path,
    },
    Increment {
        path: Path,
        value: f64,
    },
    InsertAfter {
        path: Path,
        value: serde_json::Value,
    },
}

#[derive(Clone, Debug)]
pub enum ArrayIndex {
    Head,
    Index(u32),
}

#[derive(Clone, Debug)]
enum PathElement {
    Root,
    Key(String),
    Index(ArrayIndex),
}

#[derive(Debug)]
pub struct Path(Vec<PathElement>);

impl Path {
    pub fn root() -> Path {
        Path(vec![PathElement::Root])
    }

    pub fn index(&self, index: ArrayIndex) -> Path {
        let mut elems = self.0.clone();
        elems.push(PathElement::Index(index));
        Path(elems)
    }

    pub fn key(&self, key: String) -> Path {
        let mut elems = self.0.clone();
        elems.push(PathElement::Key(key));
        Path(elems)
    }
}
