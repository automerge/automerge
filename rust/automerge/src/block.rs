use crate::storage::parse;
use crate::types::{ OpId };

pub(crate) const BLOCK_MAGIC_BYTES: [u8; 4] = [0xa2, 0x1f, 0x01, 0xd1];

#[derive(Clone, Default, Debug, PartialEq)]
pub struct Block2 {
    pub name: String,
    pub parents: Vec<String>,
}

impl Block2 {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend_from_slice(&BLOCK_MAGIC_BYTES);
        leb128::write::unsigned(&mut bytes, self.parents.len() as u64 + 1).unwrap();
        leb128::write::unsigned(&mut bytes, self.name.as_bytes().len() as u64).unwrap();
        bytes.extend_from_slice(self.name.as_bytes());
        for p in &self.parents {
            leb128::write::unsigned(&mut bytes, p.as_bytes().len() as u64).unwrap();
            bytes.extend_from_slice(p.as_bytes());
        }
        bytes
    }

    pub(crate) fn try_decode(bytes: &[u8]) -> Option<Self> {
        let mut parents = vec![];
        let mut i = parse::Input::new(bytes);

        let (num, header, name);
        let mut len;
        let mut parent;

        (i, header) = parse::take4::<()>(i).ok()?;
        if header != BLOCK_MAGIC_BYTES {
            return None;
        }
        (i, num) = parse::leb128_u64::<parse::leb128::Error>(i).ok()?;
        if num < 1 {
            return None;
        }
        (i, len) = parse::leb128_u64::<parse::leb128::Error>(i).ok()?;
        (i, name) = parse::utf_8::<parse::InvalidUtf8>(len as usize, i).ok()?;

        for _ in 0..(num - 1) {
            (i, len) = parse::leb128_u64::<parse::leb128::Error>(i).ok()?;
            (i, parent) = parse::utf_8::<parse::InvalidUtf8>(len as usize, i).ok()?;
            parents.push(parent);
        }

        Some(Block2 { name, parents })
    }
}

pub type Block = OpId;
