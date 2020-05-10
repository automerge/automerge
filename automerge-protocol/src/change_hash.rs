#[derive(Eq, PartialEq, Debug, Hash, Clone, PartialOrd, Ord, Copy)]
pub struct ChangeHash(pub [u8; 32]);

impl ChangeHash {
    pub fn new() -> Self {
        ChangeHash([0; 32])
    }
}

// TODO Should this by TryFrom? `copy_from_slice` will panic if the slice is not the
// same length as the array
impl From<&[u8]> for ChangeHash {
    fn from(bytes: &[u8]) -> Self {
        let mut array = [0; 32];
        array.copy_from_slice(bytes);
        ChangeHash(array)
    }
}
