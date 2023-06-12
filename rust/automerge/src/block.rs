use crate::storage::parse;
use crate::types::OpType;
use crate::ScalarValue;

pub(crate) const BLOCK_MAGIC_BYTES: [u8; 4] = [0xa2, 0x1f, 0x01, 0xd1];

#[derive(Clone, Debug, PartialEq)]
pub struct Block {
    pub name: String,
    pub parents: Vec<String>,
}

impl Block {
    pub(crate) fn new<S: Into<String>, P: IntoIterator<Item = S>>(name: S, parents: P) -> Self {
        Block {
            name: name.into(),
            parents: parents.into_iter().map(|p| p.into()).collect(),
        }
    }

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

        Some(Block { name, parents })
    }
}

#[test]
fn test_block_encode_decode() {
    let block1 = Block {
        name: "p".to_owned(),
        parents: vec![],
    };
    let bytes1 = block1.encode();
    let block2 = Block::try_decode(&bytes1).unwrap();

    assert_eq!(block1, block2);

    let block3 = Block {
        name: "some big long string 🐻".to_owned(),
        parents: vec!["li".to_owned(), "ol".to_owned(), "div".to_owned()],
    };
    let bytes2 = block3.encode();
    let block4 = Block::try_decode(&bytes2).unwrap();

    assert_eq!(block3, block4);

    let block5 = Block::try_decode(&bytes2[0..12]);

    assert_eq!(block5, None);
}

impl From<Block> for ScalarValue {
    fn from(b: Block) -> ScalarValue {
        ScalarValue::Bytes(b.encode())
    }
}

impl From<Option<&Block>> for OpType {
    fn from(b: Option<&Block>) -> OpType {
        if let Some(b) = b {
            OpType::Put(ScalarValue::Bytes(b.encode()))
        } else {
            OpType::Delete
        }
    }
}
