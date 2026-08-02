use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::legacy::{ElementId, Key, MarkData, ObjectId, OpId, OpType};
use crate::{ActorId, Automerge, Change, ChangeHash, ObjType, ScalarValue, TextEncoding};

/// A canonical description of the parts of a document which anonymization promises to retain.
///
/// Actor IDs, change hashes, operation IDs, and structural characters are replaced with canonical
/// indices. Private scalar data is reduced to its type and encoded shape.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ShapeSignature {
    text_encoding: TextEncoding,
    actor_count: usize,
    heads: Vec<ChangeId>,
    changes: Vec<ChangeShape>,
}

impl ShapeSignature {
    pub(super) fn new(document: &Automerge) -> Self {
        ShapeBuilder::new(document).build(document)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ChangeId {
    actor: usize,
    seq: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct ChangeShape {
    id: ChangeId,
    start_op: u64,
    dependencies: Vec<ChangeId>,
    message: Option<ContentStringShape>,
    extra_bytes: usize,
    operations: Vec<OperationShape>,
}

#[derive(Debug, PartialEq, Eq)]
struct OperationShape {
    object: ObjectShape,
    key: KeyShape,
    predecessors: Vec<CanonicalOpId>,
    insert: bool,
    action: ActionShape,
}

#[derive(Debug, PartialEq, Eq)]
enum ObjectShape {
    Root,
    Id(CanonicalOpId),
}

#[derive(Debug, PartialEq, Eq)]
enum KeyShape {
    Map(StructuralStringShape),
    Head,
    Element(CanonicalOpId),
}

#[derive(Debug, PartialEq, Eq)]
enum ActionShape {
    Make(ObjType),
    Delete,
    Increment,
    Put(ScalarShape),
    MarkBegin {
        name: StructuralStringShape,
        value: ScalarShape,
        expand: bool,
    },
    MarkEnd(bool),
}

#[derive(Debug, PartialEq, Eq)]
enum ScalarShape {
    Bytes(usize),
    String(ContentStringShape),
    Int,
    Uint,
    Float,
    Counter,
    Timestamp,
    Boolean,
    Unknown { type_code: u8, bytes: usize },
    Null,
}

#[derive(Debug, PartialEq, Eq)]
struct StructuralStringShape(Vec<StructuralCharacterShape>);

#[derive(Debug, PartialEq, Eq)]
struct StructuralCharacterShape {
    identity: usize,
    class: CharacterClass,
}

#[derive(Debug, PartialEq, Eq)]
struct ContentStringShape(Vec<ContentCharacterShape>);

#[derive(Debug, PartialEq, Eq)]
enum ContentCharacterShape {
    /// Whitespace and ASCII control characters are deliberately retained verbatim.
    Retained(char),
    Replaced(CharacterClass),
}

#[derive(Debug, PartialEq, Eq)]
enum CharacterClass {
    PrintableAscii,
    AsciiControl,
    Unicode { utf8: u8, utf16: u8 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct CanonicalOpId {
    counter: u64,
    actor: usize,
}

struct ShapeBuilder {
    actors: HashMap<ActorId, usize>,
    changes: HashMap<ChangeHash, ChangeId>,
    structural_characters: HashMap<char, usize>,
}

impl ShapeBuilder {
    fn new(document: &Automerge) -> Self {
        let changes = document.get_changes(&[]).unwrap();
        let actors = actor_ranks(&changes);
        let change_ids = changes
            .iter()
            .map(|change| {
                (
                    change.hash(),
                    ChangeId {
                        actor: actors[change.actor_id()],
                        seq: change.seq(),
                    },
                )
            })
            .collect();
        Self {
            actors,
            changes: change_ids,
            structural_characters: HashMap::new(),
        }
    }

    fn build(mut self, document: &Automerge) -> ShapeSignature {
        let mut changes = document.get_changes(&[]).unwrap();
        changes.sort_unstable_by_key(|change| self.changes[&change.hash()]);

        let changes = changes.iter().map(|change| self.change(change)).collect();
        // hashes, not ids: this map is keyed by the change's hash, and
        // `get_heads` is ids on this branch
        let mut heads = document
            .get_head_hashes()
            .iter()
            .map(|hash| self.changes[hash])
            .collect::<Vec<_>>();
        heads.sort_unstable();

        ShapeSignature {
            text_encoding: document.text_encoding(),
            actor_count: self.actors.len(),
            heads,
            changes,
        }
    }

    fn change(&mut self, change: &Change) -> ChangeShape {
        let expanded = change.decode();
        let mut dependencies = change
            .deps()
            .iter()
            .map(|hash| self.changes[hash])
            .collect::<Vec<_>>();
        dependencies.sort_unstable();

        ChangeShape {
            id: self.changes[&change.hash()],
            start_op: change.start_op().get(),
            dependencies,
            message: change.message().map(content_string_shape),
            extra_bytes: change.extra_bytes().len(),
            operations: expanded
                .operations
                .iter()
                .map(|operation| self.operation(operation))
                .collect(),
        }
    }

    fn operation(&mut self, operation: &crate::legacy::Op) -> OperationShape {
        let mut predecessors = operation
            .pred
            .iter()
            .map(|id| self.op_id(id))
            .collect::<Vec<_>>();
        predecessors.sort_unstable();

        OperationShape {
            object: match &operation.obj {
                ObjectId::Root => ObjectShape::Root,
                ObjectId::Id(id) => ObjectShape::Id(self.op_id(id)),
            },
            key: match &operation.key {
                Key::Map(key) => KeyShape::Map(self.structural_string(key)),
                Key::Seq(ElementId::Head) => KeyShape::Head,
                Key::Seq(ElementId::Id(id)) => KeyShape::Element(self.op_id(id)),
            },
            predecessors,
            insert: operation.insert,
            action: self.action(&operation.action),
        }
    }

    fn action(&mut self, action: &OpType) -> ActionShape {
        match action {
            OpType::Make(object_type) => ActionShape::Make(*object_type),
            OpType::Delete => ActionShape::Delete,
            OpType::Increment(_) => ActionShape::Increment,
            OpType::Put(value) => ActionShape::Put(scalar_shape(value)),
            OpType::MarkBegin(MarkData {
                name,
                value,
                expand,
            }) => ActionShape::MarkBegin {
                name: self.structural_string(name),
                value: scalar_shape(value),
                expand: *expand,
            },
            OpType::MarkEnd(expand) => ActionShape::MarkEnd(*expand),
        }
    }

    fn structural_string(&mut self, value: &str) -> StructuralStringShape {
        StructuralStringShape(
            value
                .chars()
                .map(|character| {
                    let identity =
                        if let Some(identity) = self.structural_characters.get(&character) {
                            *identity
                        } else {
                            let identity = self.structural_characters.len();
                            self.structural_characters.insert(character, identity);
                            identity
                        };
                    StructuralCharacterShape {
                        identity,
                        class: character_class(character),
                    }
                })
                .collect(),
        )
    }

    fn op_id(&self, id: &OpId) -> CanonicalOpId {
        CanonicalOpId {
            counter: id.counter(),
            actor: self.actors[id.actor()],
        }
    }
}

fn scalar_shape(value: &ScalarValue) -> ScalarShape {
    match value {
        ScalarValue::Bytes(value) => ScalarShape::Bytes(value.len()),
        ScalarValue::Str(value) => ScalarShape::String(content_string_shape(value)),
        ScalarValue::Int(_) => ScalarShape::Int,
        ScalarValue::Uint(_) => ScalarShape::Uint,
        ScalarValue::F64(_) => ScalarShape::Float,
        ScalarValue::Counter(_) => ScalarShape::Counter,
        ScalarValue::Timestamp(_) => ScalarShape::Timestamp,
        ScalarValue::Boolean(_) => ScalarShape::Boolean,
        ScalarValue::Unknown { type_code, bytes } => ScalarShape::Unknown {
            type_code: *type_code,
            bytes: bytes.len(),
        },
        ScalarValue::Null => ScalarShape::Null,
    }
}

fn content_string_shape(value: &str) -> ContentStringShape {
    ContentStringShape(
        value
            .chars()
            .map(|character| {
                if character.is_whitespace() || character.is_ascii_control() {
                    ContentCharacterShape::Retained(character)
                } else {
                    ContentCharacterShape::Replaced(character_class(character))
                }
            })
            .collect(),
    )
}

fn character_class(character: char) -> CharacterClass {
    if character.is_ascii_control() {
        CharacterClass::AsciiControl
    } else if character.is_ascii() {
        CharacterClass::PrintableAscii
    } else {
        CharacterClass::Unicode {
            utf8: character.len_utf8() as u8,
            utf16: character.len_utf16() as u8,
        }
    }
}

fn actor_ranks(changes: &[Change]) -> HashMap<ActorId, usize> {
    changes
        .iter()
        .flat_map(Change::actors)
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .enumerate()
        .map(|(index, actor)| (actor, index))
        .collect()
}

/// Check data-bearing fields separately from [`ShapeSignature`], which deliberately erases them.
pub(super) fn assert_private_data_changed(source: &Automerge, anonymized: &Automerge) {
    let source_changes = canonical_changes(source);
    let anonymized_changes = canonical_changes(anonymized);
    assert_eq!(source_changes.len(), anonymized_changes.len());

    for (id, source) in source_changes {
        let anonymized = &anonymized_changes[&id];
        assert_ne!(source.hash(), anonymized.hash(), "unchanged hash in {id:?}");
        assert_ne!(
            source.actor_id(),
            anonymized.actor_id(),
            "unchanged actor in {id:?}"
        );
        assert_ne!(
            source.timestamp(),
            anonymized.timestamp(),
            "unchanged timestamp in {id:?}"
        );
        if let (Some(source), Some(anonymized)) = (source.message(), anonymized.message()) {
            assert_content_changed(source, anonymized, "change message");
        }
        assert_bytes_changed(
            source.extra_bytes(),
            anonymized.extra_bytes(),
            "change extra bytes",
        );

        let source = source.decode();
        let anonymized = anonymized.decode();
        assert_eq!(source.operations.len(), anonymized.operations.len());
        for (index, (source, anonymized)) in source
            .operations
            .iter()
            .zip(&anonymized.operations)
            .enumerate()
        {
            if let (Key::Map(source), Key::Map(anonymized)) = (&source.key, &anonymized.key) {
                assert_structural_string_changed(source, anonymized, "map key");
            }
            match (&source.action, &anonymized.action) {
                (OpType::Put(source), OpType::Put(anonymized)) => {
                    assert_scalar_changed(source, anonymized)
                }
                (OpType::Increment(source), OpType::Increment(anonymized)) => {
                    assert_ne!(
                        source, anonymized,
                        "unchanged increment in {id:?} op {index}"
                    )
                }
                (OpType::MarkBegin(source), OpType::MarkBegin(anonymized)) => {
                    assert_structural_string_changed(&source.name, &anonymized.name, "mark name");
                    assert_scalar_changed(&source.value, &anonymized.value);
                }
                (OpType::Make(_), OpType::Make(_))
                | (OpType::Delete, OpType::Delete)
                | (OpType::MarkEnd(_), OpType::MarkEnd(_)) => {}
                _ => panic!("action shape differs in {id:?} op {index}"),
            }
        }
    }
}

fn canonical_changes(document: &Automerge) -> BTreeMap<ChangeId, Change> {
    let changes = document.get_changes(&[]).unwrap();
    let actors = actor_ranks(&changes);
    changes
        .into_iter()
        .map(|change| {
            (
                ChangeId {
                    actor: actors[change.actor_id()],
                    seq: change.seq(),
                },
                change,
            )
        })
        .collect()
}

fn assert_scalar_changed(source: &ScalarValue, anonymized: &ScalarValue) {
    match (source, anonymized) {
        (ScalarValue::Bytes(source), ScalarValue::Bytes(anonymized)) => {
            assert_bytes_changed(source, anonymized, "scalar bytes")
        }
        (ScalarValue::Str(source), ScalarValue::Str(anonymized)) => {
            assert_content_changed(source, anonymized, "scalar string")
        }
        (ScalarValue::Int(source), ScalarValue::Int(anonymized)) => {
            assert_ne!(source, anonymized)
        }
        (ScalarValue::Uint(source), ScalarValue::Uint(anonymized)) => {
            assert_ne!(source, anonymized)
        }
        (ScalarValue::F64(source), ScalarValue::F64(anonymized)) => {
            assert_ne!(source.to_bits(), anonymized.to_bits())
        }
        (ScalarValue::Counter(source), ScalarValue::Counter(anonymized)) => {
            assert_ne!(i64::from(source), i64::from(anonymized))
        }
        (ScalarValue::Timestamp(source), ScalarValue::Timestamp(anonymized)) => {
            assert_ne!(source, anonymized)
        }
        // A random boolean can equal its source, and null has no alternative of the same type.
        (ScalarValue::Boolean(_), ScalarValue::Boolean(_))
        | (ScalarValue::Null, ScalarValue::Null) => {}
        (
            ScalarValue::Unknown { bytes: source, .. },
            ScalarValue::Unknown {
                bytes: anonymized, ..
            },
        ) => assert_bytes_changed(source, anonymized, "unknown scalar bytes"),
        _ => panic!("scalar shape differs"),
    }
}

fn assert_structural_string_changed(source: &str, anonymized: &str, label: &str) {
    assert_eq!(source.chars().count(), anonymized.chars().count());
    for (source, anonymized) in source.chars().zip(anonymized.chars()) {
        assert_ne!(source, anonymized, "unchanged character in {label}");
    }
}

fn assert_content_changed(source: &str, anonymized: &str, label: &str) {
    assert_eq!(source.chars().count(), anonymized.chars().count());
    for (source, anonymized) in source.chars().zip(anonymized.chars()) {
        if source.is_whitespace() || source.is_ascii_control() {
            assert_eq!(source, anonymized, "retained character changed in {label}");
        } else {
            assert_ne!(source, anonymized, "unchanged character in {label}");
        }
    }
}

fn assert_bytes_changed(source: &[u8], anonymized: &[u8], label: &str) {
    assert_eq!(source.len(), anonymized.len());
    for (source, anonymized) in source.iter().zip(anonymized) {
        assert_ne!(source, anonymized, "unchanged byte in {label}");
    }
}
