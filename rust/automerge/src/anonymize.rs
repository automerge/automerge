use crate::legacy::{ElementId, Key, MarkData, ObjectId, OpId, OpType};
use crate::{ActorId, Automerge, AutomergeError, Change, ChangeHash, ScalarValue};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::RngExt;
use std::collections::{BTreeSet, HashMap};

const SYNTHETIC_TEXT: &[u8] = b"loremipsumdolorsitametconsecteturadipiscingelit";

/// An error encountered while anonymizing a document.
#[derive(Debug, thiserror::Error)]
pub enum AnonymizeError {
    /// A change appeared before one of its dependencies.
    #[error("change {change} appeared before dependency {dependency}")]
    MissingDependency {
        /// The change being anonymized.
        change: ChangeHash,
        /// The dependency which has not yet been anonymized.
        dependency: ChangeHash,
    },
    /// The rewritten changes could not be applied.
    #[error(transparent)]
    Apply(#[from] AutomergeError),
}

/// Return a new document anonymized with a fresh random seed.
pub fn anonymize(document: &Automerge) -> Result<Automerge, AnonymizeError> {
    Anonymization::new(rand::make_rng()).anonymize(document)
}

struct Anonymization {
    rng: StdRng,
    structural_permutations: StructuralPermutations,
    content_permutations: StructuralPermutations,
    synthetic_text: Vec<u8>,
    synthetic_position: usize,
}

impl Anonymization {
    fn new(mut rng: StdRng) -> Self {
        let mut synthetic_text = SYNTHETIC_TEXT.to_vec();
        synthetic_text.shuffle(&mut rng);
        let synthetic_position = rng.random_range(0..synthetic_text.len());
        Self {
            rng,
            structural_permutations: StructuralPermutations::default(),
            content_permutations: StructuralPermutations::default(),
            synthetic_text,
            synthetic_position,
        }
    }

    #[cfg(test)]
    fn from_seed(seed: [u8; 32]) -> Self {
        use rand::SeedableRng as _;

        Self::new(StdRng::from_seed(seed))
    }

    fn anonymize(mut self, document: &Automerge) -> Result<Automerge, AnonymizeError> {
        let changes = document.get_changes(&[])?;
        let actor_map = self.actor_map(&changes);
        let mut change_hashes = HashMap::<ChangeHash, ChangeHash>::new();
        let mut anonymized = Automerge::new_with_encoding(document.text_encoding());
        // Anonymizing replays a chain of rewritten changes into this
        // document, and `apply_changes` over a chain is only reliable in
        // audit mode (HASHLESS.md). The caller cannot enable it — the
        // document is created here — and on an empty one it is free.
        anonymized
            .enable_audit_mode()
            .expect("an empty document has no changes to hash");

        for change in changes {
            let old_hash = change.hash();
            let mut expanded = change.decode();
            expanded.hash = None;
            expanded.deps = expanded
                .deps
                .iter()
                .map(|dependency| {
                    change_hashes.get(dependency).copied().ok_or(
                        AnonymizeError::MissingDependency {
                            change: old_hash,
                            dependency: *dependency,
                        },
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            expanded.actor_id = mapped_actor(&actor_map, &expanded.actor_id);
            expanded.time = self.random_i64_other_than(expanded.time);
            expanded.message = expanded
                .message
                .as_deref()
                .map(|message| self.anonymize_content_string(message));
            expanded.extra_bytes = self.anonymize_bytes(&expanded.extra_bytes);

            for operation in &mut expanded.operations {
                self.anonymize_operation(operation, &actor_map);
            }

            let anonymized_change = Change::from(expanded);
            let anonymized_hash = anonymized_change.hash();
            anonymized.apply_changes([anonymized_change])?;
            change_hashes.insert(old_hash, anonymized_hash);
        }

        Ok(anonymized)
    }

    fn actor_map(&mut self, changes: &[Change]) -> HashMap<ActorId, ActorId> {
        let actors = changes
            .iter()
            .flat_map(Change::actors)
            .cloned()
            .collect::<BTreeSet<_>>();

        loop {
            let prefix = self.rng.random::<[u8; 8]>();
            let mapped = actors
                .iter()
                .enumerate()
                .map(|(rank, actor)| {
                    let mut anonymized = [0_u8; 16];
                    anonymized[..8].copy_from_slice(&prefix);
                    anonymized[8..].copy_from_slice(&(rank as u64).to_be_bytes());
                    (actor.clone(), ActorId::from(anonymized))
                })
                .collect::<HashMap<_, _>>();
            if mapped.values().all(|actor| !actors.contains(actor)) {
                return mapped;
            }
        }
    }

    fn anonymize_operation(
        &mut self,
        operation: &mut crate::legacy::Op,
        actors: &HashMap<ActorId, ActorId>,
    ) {
        map_object_id(&mut operation.obj, actors);
        match &mut operation.key {
            Key::Map(key) => *key = self.anonymize_structural_string(key).into(),
            Key::Seq(ElementId::Id(id)) => map_op_id(id, actors),
            Key::Seq(ElementId::Head) => {}
        }
        operation.pred = operation
            .pred
            .iter()
            .cloned()
            .map(|mut predecessor| {
                map_op_id(&mut predecessor, actors);
                predecessor
            })
            .collect();

        match &mut operation.action {
            OpType::Put(value) => *value = self.anonymize_scalar(value),
            OpType::Increment(value) => *value = self.random_i64_other_than(*value),
            OpType::MarkBegin(MarkData { name, value, .. }) => {
                *name = self.anonymize_structural_string(name).into();
                *value = self.anonymize_scalar(value);
            }
            OpType::Make(_) | OpType::Delete | OpType::MarkEnd(_) => {}
        }
    }

    fn anonymize_scalar(&mut self, value: &ScalarValue) -> ScalarValue {
        match value {
            ScalarValue::Bytes(bytes) => ScalarValue::Bytes(self.anonymize_bytes(bytes)),
            ScalarValue::Str(value) => {
                ScalarValue::from(self.anonymize_content_string(value.as_str()))
            }
            ScalarValue::Int(value) => ScalarValue::Int(self.random_i64_other_than(*value)),
            ScalarValue::Uint(value) => ScalarValue::Uint(self.random_u64_other_than(*value)),
            ScalarValue::F64(value) => ScalarValue::F64(self.random_f64_other_than(*value)),
            ScalarValue::Counter(value) => {
                ScalarValue::counter(self.random_i64_other_than(i64::from(value)))
            }
            ScalarValue::Timestamp(value) => {
                ScalarValue::Timestamp(self.random_i64_other_than(*value))
            }
            ScalarValue::Boolean(_) => ScalarValue::Boolean(self.rng.random()),
            ScalarValue::Unknown { type_code, bytes } => ScalarValue::Unknown {
                type_code: *type_code,
                bytes: self.anonymize_bytes(bytes),
            },
            ScalarValue::Null => ScalarValue::Null,
        }
    }

    fn random_i64_other_than(&mut self, original: i64) -> i64 {
        loop {
            // Signed integer columns use delta encoding. Sampling from the i32 domain keeps the
            // difference between any two generated values representable as an i64 while still
            // producing independent, varied values for run-length and delta encoding.
            let replacement = i64::from(self.rng.random::<i32>());
            if replacement != original {
                return replacement;
            }
        }
    }

    fn random_u64_other_than(&mut self, original: u64) -> u64 {
        loop {
            let replacement = self.rng.random();
            if replacement != original {
                return replacement;
            }
        }
    }

    fn random_f64_other_than(&mut self, original: f64) -> f64 {
        loop {
            // `rand` samples finite values from 0..1, avoiding NaNs and infinities while still
            // producing independently encoded values for each occurrence.
            let replacement = self.rng.random::<f64>();
            if replacement.to_bits() != original.to_bits() {
                return replacement;
            }
        }
    }

    fn anonymize_structural_string(&mut self, value: &str) -> String {
        value
            .chars()
            .map(|character| {
                self.structural_permutations
                    .replace(character, &mut self.rng)
            })
            .collect()
    }

    fn anonymize_content_string(&mut self, value: &str) -> String {
        let mut result = String::with_capacity(value.len());
        for character in value.chars() {
            if character.is_whitespace() || character.is_ascii_control() {
                result.push(character);
            } else if character.is_ascii() {
                result.push(char::from(self.random_synthetic_byte(character as u8)));
            } else {
                result.push(self.content_permutations.replace(character, &mut self.rng));
            }
        }
        result
    }

    fn anonymize_bytes(&mut self, value: &[u8]) -> Vec<u8> {
        value
            .iter()
            .map(|source| self.random_synthetic_byte(*source))
            .collect()
    }

    fn random_synthetic_byte(&mut self, original: u8) -> u8 {
        loop {
            let replacement = self.synthetic_text[self.synthetic_position];
            self.synthetic_position = (self.synthetic_position + 1) % self.synthetic_text.len();
            if replacement != original {
                return replacement;
            }
        }
    }
}

/// Lazily generated, seeded substitution tables for each character encoding class.
///
/// The rank conversion helpers below are deterministic, but they only index these tables. The
/// source rank is mapped through a randomly shuffled derangement before being converted back into a
/// character. This deliberately preserves character equality patterns and is consequently a
/// substitution cipher, not cryptographic protection.
#[derive(Default)]
struct StructuralPermutations {
    printable_ascii: Option<Vec<u32>>,
    ascii_control: Option<Vec<u32>>,
    two_byte: Option<Vec<u32>>,
    three_byte: Option<Vec<u32>>,
    four_byte: Option<Vec<u32>>,
}

impl StructuralPermutations {
    fn replace(&mut self, character: char, rng: &mut StdRng) -> char {
        let (alphabet, source_rank, alphabet_size) = structural_character_rank(character);
        let permutation = match alphabet {
            StructuralAlphabet::PrintableAscii => &mut self.printable_ascii,
            StructuralAlphabet::AsciiControl => &mut self.ascii_control,
            StructuralAlphabet::TwoByte => &mut self.two_byte,
            StructuralAlphabet::ThreeByte => &mut self.three_byte,
            StructuralAlphabet::FourByte => &mut self.four_byte,
        }
        .get_or_insert_with(|| random_derangement(alphabet_size, rng));
        let replacement_rank = permutation[source_rank as usize];
        structural_character_from_rank(character, replacement_rank)
    }
}

#[derive(Clone, Copy)]
enum StructuralAlphabet {
    PrintableAscii,
    AsciiControl,
    TwoByte,
    ThreeByte,
    FourByte,
}

fn random_derangement(count: u32, rng: &mut StdRng) -> Vec<u32> {
    debug_assert!(count > 1);
    loop {
        let mut permutation = (0..count).collect::<Vec<_>>();
        permutation.shuffle(rng);
        if permutation
            .iter()
            .enumerate()
            .all(|(index, replacement)| index as u32 != *replacement)
        {
            return permutation;
        }
    }
}

// Locate a character within an alphabet whose members have the same encoded width. Randomization
// happens when `StructuralPermutations::replace` maps this rank through a seeded permutation.
fn structural_character_rank(character: char) -> (StructuralAlphabet, u32, u32) {
    let codepoint = character as u32;
    match character.len_utf8() {
        1 if (0x20..=0x7e).contains(&codepoint) => (
            StructuralAlphabet::PrintableAscii,
            codepoint - 0x20,
            0x7e - 0x20 + 1,
        ),
        1 if codepoint < 0x20 => (StructuralAlphabet::AsciiControl, codepoint, 0x21),
        1 => (StructuralAlphabet::AsciiControl, 0x20, 0x21),
        2 => (StructuralAlphabet::TwoByte, codepoint - 0x80, 0x800 - 0x80),
        3 if codepoint < 0xd800 => (
            StructuralAlphabet::ThreeByte,
            codepoint - 0x800,
            0xd800 - 0x800 + 0x2000,
        ),
        3 => (
            StructuralAlphabet::ThreeByte,
            0xd800 - 0x800 + codepoint - 0xe000,
            0xd800 - 0x800 + 0x2000,
        ),
        4 => (
            StructuralAlphabet::FourByte,
            codepoint - 0x10000,
            0x110000 - 0x10000,
        ),
        _ => unreachable!("Rust char values use one to four UTF-8 bytes"),
    }
}

fn structural_character_from_rank(original: char, rank: u32) -> char {
    let codepoint = match original.len_utf8() {
        1 if (' '..='~').contains(&original) => 0x20 + rank,
        1 if original < ' ' => rank,
        1 => {
            if rank < 0x20 {
                rank
            } else {
                0x7f
            }
        }
        2 => 0x80 + rank,
        3 if rank < 0xd800 - 0x800 => 0x800 + rank,
        3 => 0xe000 + rank - (0xd800 - 0x800),
        4 => 0x10000 + rank,
        _ => unreachable!("Rust char values use one to four UTF-8 bytes"),
    };
    char::from_u32(codepoint).expect("character rank should map to a valid Unicode scalar")
}

fn mapped_actor(actors: &HashMap<ActorId, ActorId>, actor: &ActorId) -> ActorId {
    actors
        .get(actor)
        .expect("every referenced actor should be present in the change actor table")
        .clone()
}

fn map_op_id(id: &mut OpId, actors: &HashMap<ActorId, ActorId>) {
    id.1 = mapped_actor(actors, &id.1);
}

fn map_object_id(id: &mut ObjectId, actors: &HashMap<ActorId, ActorId>) {
    if let ObjectId::Id(id) = id {
        map_op_id(id, actors);
    }
}

#[cfg(test)]
mod fuzz;
#[cfg(test)]
mod shape;

#[cfg(test)]
mod tests {
    use super::{shape::ShapeSignature, Anonymization, Key, OpType};
    use crate::transaction::{CommitOptions, Transactable};
    use crate::{AutoCommit, Automerge, ObjType, ReadDoc, ScalarValue, ROOT};
    use std::collections::{HashMap, HashSet};

    #[test]
    fn repeated_numeric_values_receive_varied_replacements() {
        let mut anonymization = Anonymization::from_seed([2; 32]);

        let signed = (0..16)
            .map(|_| anonymization.random_i64_other_than(42))
            .collect::<HashSet<_>>();
        let unsigned = (0..16)
            .map(|_| anonymization.random_u64_other_than(42))
            .collect::<HashSet<_>>();
        let floats = (0..16)
            .map(|_| anonymization.random_f64_other_than(42.0).to_bits())
            .collect::<HashSet<_>>();

        assert!(signed.len() > 1 && !signed.contains(&42));
        assert!(unsigned.len() > 1 && !unsigned.contains(&42));
        assert!(floats.len() > 1 && !floats.contains(&42.0_f64.to_bits()));
    }

    #[test]
    fn structural_strings_are_seeded_unique_and_preserve_encoded_lengths() {
        let mut first = Anonymization::from_seed([3; 32]);
        let mut second = Anonymization::from_seed([4; 32]);
        let source = ["name", "email", "a", "b", "é", "界", "😀"];
        let anonymized = source.map(|value| first.anonymize_structural_string(value));
        let differently_anonymized = source.map(|value| second.anonymize_structural_string(value));

        assert_ne!(anonymized, differently_anonymized);
        assert_eq!(
            anonymized
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            source.len()
        );
        for (source, anonymized) in source.into_iter().zip(anonymized) {
            assert_ne!(source, anonymized);
            assert_eq!(source.len(), anonymized.len());
            assert_eq!(
                source.encode_utf16().count(),
                anonymized.encode_utf16().count()
            );
        }
    }

    #[test]
    fn anonymizes_data_while_preserving_history_and_shape() {
        let mut source = AutoCommit::new();
        source.put(ROOT, "private-key", "secret value").unwrap();
        source.put(ROOT, "private-bytes", vec![1, 2, 3, 4]).unwrap();
        source.put(ROOT, "private-number", 42_i64).unwrap();
        source
            .put(ROOT, "private-counter", ScalarValue::counter(10))
            .unwrap();
        let text = source
            .put_object(ROOT, "private-text", ObjType::Text)
            .unwrap();
        source
            .splice_text(&text, 0, 0, "Meeting with Alice 👋\nTomorrow")
            .unwrap();
        source.commit_with(
            CommitOptions::default()
                .with_message("private commit message")
                .with_time(1_700_000_000),
        );
        source.increment(ROOT, "private-counter", 5).unwrap();
        source.put(ROOT, "private-key", "another secret").unwrap();
        source.commit();

        // anonymizing reads the source's changes, which needs its hash
        // graph — a default load does not keep one
        let source =
            Automerge::load_with_options(&source.save(), crate::LoadOptions::new().with_audit_mode())
                .unwrap();
        let anonymized = Anonymization::from_seed([7; 32])
            .anonymize(&source)
            .unwrap();
        let anonymized_again = Anonymization::from_seed([7; 32])
            .anonymize(&source)
            .unwrap();
        let differently_anonymized = Anonymization::from_seed([8; 32])
            .anonymize(&source)
            .unwrap();

        assert_eq!(anonymized.save(), anonymized_again.save());
        assert_ne!(anonymized.save(), differently_anonymized.save());
        assert_ne!(anonymized.save(), source.save());
        Automerge::load(&anonymized.save()).unwrap();
        assert_eq!(
            ShapeSignature::new(&source),
            ShapeSignature::new(&anonymized)
        );

        let source_changes = source.get_changes(&[]).unwrap();
        let anonymized_changes = anonymized.get_changes(&[]).unwrap();
        assert_eq!(source_changes.len(), anonymized_changes.len());
        let source_indices = source_changes
            .iter()
            .enumerate()
            .map(|(index, change)| (change.hash(), index))
            .collect::<HashMap<_, _>>();
        let anonymized_indices = anonymized_changes
            .iter()
            .enumerate()
            .map(|(index, change)| (change.hash(), index))
            .collect::<HashMap<_, _>>();
        for (source, anonymized) in source_changes.iter().zip(&anonymized_changes) {
            assert_eq!(source.len(), anonymized.len());
            assert_eq!(source.seq(), anonymized.seq());
            assert_eq!(source.start_op(), anonymized.start_op());
            assert_ne!(source.actor_id(), anonymized.actor_id());
            let source_deps = source
                .deps()
                .iter()
                .map(|hash| source_indices[hash])
                .collect::<Vec<_>>();
            let anonymized_deps = anonymized
                .deps()
                .iter()
                .map(|hash| anonymized_indices[hash])
                .collect::<Vec<_>>();
            assert_eq!(source_deps, anonymized_deps);
            if source.timestamp() != 0 {
                assert_ne!(source.timestamp(), anonymized.timestamp());
            }
            if source.message().is_some() {
                assert_ne!(source.message(), anonymized.message());
            }
        }

        assert!(anonymized.get(ROOT, "private-key").unwrap().is_none());
        assert!(anonymized.get(ROOT, "private-text").unwrap().is_none());
        let text_id = anonymized
            .keys(ROOT)
            .find_map(|key| match anonymized.get(ROOT, key).unwrap() {
                Some((crate::Value::Object(ObjType::Text), object)) => Some(object),
                _ => None,
            })
            .unwrap();
        let source_text = source.text(&text).unwrap();
        let anonymized_text = anonymized.text(&text_id).unwrap();
        assert_ne!(source_text, anonymized_text);
        assert_eq!(source_text.len(), anonymized_text.len());
        assert_eq!(
            source_text.encode_utf16().count(),
            anonymized_text.encode_utf16().count()
        );

        for change in &anonymized_changes {
            let expanded = change.decode();
            assert_ne!(expanded.message.as_deref(), Some("private commit message"));
            for operation in expanded.operations {
                if let Key::Map(key) = operation.key {
                    assert!(!key.starts_with("private"));
                }
                match operation.action {
                    OpType::Put(ScalarValue::Str(value)) => {
                        assert_ne!(value.as_str(), "secret value");
                        assert_ne!(value.as_str(), "another secret");
                    }
                    OpType::Put(ScalarValue::Bytes(value)) => {
                        assert_ne!(value, vec![1, 2, 3, 4]);
                    }
                    _ => {}
                }
            }
        }
    }
}
