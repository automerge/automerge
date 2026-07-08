#![no_main]

use automerge::Automerge;
use libfuzzer_sys::arbitrary::{Arbitrary, Result, Unstructured};
use libfuzzer_sys::fuzz_target;
use sha2::{Digest, Sha256};

#[derive(Debug)]
struct DocumentChunk {
    bytes: Vec<u8>,
}

fn add_header(typ: u8, data: &[u8]) -> Vec<u8> {
    let mut input = vec![typ];
    leb128::write::unsigned(&mut input, data.len() as u64).unwrap();
    input.extend(data.as_ref());
    let hash_result = Sha256::digest(input.clone());
    let array: [u8; 32] = hash_result.into();

    let mut out = vec![133, 111, 74, 131, array[0], array[1], array[2], array[3]];
    out.extend(input);
    out
}

impl<'a> Arbitrary<'a> for DocumentChunk {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        let input = u.bytes(u.len())?;
        let contents = add_header(0, input);

        Ok(DocumentChunk { bytes: contents })
    }
}

fuzz_target!(|doc: DocumentChunk| {
    let _ = Automerge::load(&doc.bytes);
});
