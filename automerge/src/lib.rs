
#[derive(Debug, PartialEq, Clone)]
pub struct Automerge {
    id: usize
}

impl Automerge {
    pub fn new() -> Self { Automerge { id: 0 } }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
