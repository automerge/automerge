use std::convert::TryFrom;

use crate::{error::InvalidChangeHashSlice, ChangeHash};

impl TryFrom<&[u8]> for ChangeHash {
    type Error = InvalidChangeHashSlice;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() != 32 {
            Err(InvalidChangeHashSlice(Vec::from(bytes)))
        } else {
            let mut array = [0; 32];
            array.copy_from_slice(bytes);
            Ok(ChangeHash(array))
        }
    }
}
