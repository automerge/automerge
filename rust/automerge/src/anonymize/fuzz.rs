use proptest::prelude::*;

use super::{
    shape::{assert_private_data_changed, ShapeSignature},
    Anonymization,
};
use crate::marks::{ExpandMark, Mark};
use crate::transaction::{CommitOptions, Transactable};
use crate::{ActorId, AutoCommit, Automerge, ObjId, ObjType, ReadDoc, ScalarValue, ROOT};

const REPLICA_COUNT: usize = 3;
const ROOT_KEYS: [&str; 4] = ["alpha", "beta", "gamma", "delta"];

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    #[cfg_attr(not(feature = "deep_fuzz"), ignore = "deep fuzz: run with --features deep_fuzz")]
    fn randomized_documents_retain_their_shape(
        program in proptest::collection::vec(any::<u8>(), 0..192),
        seed in any::<[u8; 32]>(),
    ) {
        let source = document_from_program(&program);
        let source_shape = ShapeSignature::new(&source);
        let anonymized = Anonymization::from_seed(seed).anonymize(&source).unwrap();

        prop_assert_eq!(source_shape, ShapeSignature::new(&anonymized));
        assert_private_data_changed(&source, &anonymized);

        let reloaded = load_audit(&anonymized.save());
        prop_assert_eq!(ShapeSignature::new(&anonymized), ShapeSignature::new(&reloaded));
    }
}

fn document_from_program(program: &[u8]) -> Automerge {
    let mut base = AutoCommit::new();
    // Merging *from* a document reconstructs its changes, which needs
    // its dep hashes — and outside audit mode a commit that forms a
    // fragment frees them (see HASHLESS.md, "change emission is
    // stochastic outside audit mode"). A fuzz program long enough to be
    // interesting will hit that, so the replicas keep their hashes.
    base.enable_audit_mode().unwrap();
    base.set_actor(actor(0)).unwrap();
    let list = base.put_object(ROOT, "items", ObjType::List).unwrap();
    let text = base.put_object(ROOT, "notes", ObjType::Text).unwrap();
    base.put(ROOT, "counter", ScalarValue::counter(0)).unwrap();
    base.commit_with(
        CommitOptions::default()
            .with_message("initial private data")
            .with_time(1_700_000_000),
    );

    let mut replicas = (0..REPLICA_COUNT)
        .map(|index| {
            let mut replica = base.fork();
            replica.set_actor(actor(index as u8 + 1)).unwrap();
            replica
        })
        .collect::<Vec<_>>();

    for instruction in program.chunks(6) {
        apply_instruction(&mut replicas, &list, &text, instruction);
    }

    for index in 1..replicas.len() {
        let mut incoming = replicas[index].fork();
        replicas[0].merge(&mut incoming).unwrap();
    }

    load_audit(&replicas[0].save())
}

/// Anonymization reads a document's changes, which needs its hash graph.
fn load_audit(bytes: &[u8]) -> Automerge {
    Automerge::load_with_options(bytes, crate::LoadOptions::new().with_audit_mode()).unwrap()
}

fn apply_instruction(replicas: &mut [AutoCommit], list: &ObjId, text: &ObjId, instruction: &[u8]) {
    let byte = |index| instruction.get(index).copied().unwrap_or_default();
    let opcode = byte(0) % 11;
    let replica_index = byte(1) as usize % replicas.len();
    let a = byte(2);
    let b = byte(3);
    let c = byte(4);
    let d = byte(5);

    if opcode == 9 {
        let source_index = a as usize % replicas.len();
        if source_index != replica_index {
            let mut incoming = replicas[source_index].fork();
            replicas[replica_index].merge(&mut incoming).unwrap();
        }
        return;
    }

    let document = &mut replicas[replica_index];
    match opcode {
        0 => {
            document
                .put(
                    ROOT,
                    ROOT_KEYS[a as usize % ROOT_KEYS.len()],
                    scalar(b, c, d),
                )
                .unwrap();
        }
        1 => {
            document
                .delete(ROOT, ROOT_KEYS[a as usize % ROOT_KEYS.len()])
                .unwrap();
        }
        2 => {
            let length = document.length(list);
            let index = a as usize % (length + 1);
            document.insert(list, index, scalar(b, c, d)).unwrap();
        }
        3 => {
            let length = document.length(list);
            if length > 0 {
                document
                    .put(list, a as usize % length, scalar(b, c, d))
                    .unwrap();
            }
        }
        4 => {
            let length = document.length(list);
            if length > 0 {
                document.delete(list, a as usize % length).unwrap();
            }
        }
        5 => {
            let length = document.length(text);
            let index = a as usize % (length + 1);
            let value = ["a", "Z", " ", "\n"][b as usize % 4];
            document.splice_text(text, index, 0, value).unwrap();
        }
        6 => {
            let length = document.length(text);
            if length > 0 {
                document
                    .splice_text(text, a as usize % length, 1, "")
                    .unwrap();
            }
        }
        7 => {
            let increment = i64::from(i8::from_ne_bytes([a]));
            let _ = document.increment(ROOT, "counter", increment);
        }
        8 => {
            let length = document.length(text);
            if length > 0 {
                let start = a as usize % length;
                let end = start + 1 + b as usize % (length - start);
                let expand = match c % 4 {
                    0 => ExpandMark::Before,
                    1 => ExpandMark::After,
                    2 => ExpandMark::Both,
                    _ => ExpandMark::None,
                };
                document
                    .mark(
                        text,
                        Mark::new(format!("mark-{}", d % 4), scalar(b, c, d), start, end),
                        expand,
                    )
                    .unwrap();
            }
        }
        10 => {
            let object_type = match b % 4 {
                0 => ObjType::Map,
                1 => ObjType::Table,
                2 => ObjType::List,
                _ => ObjType::Text,
            };
            document
                .put_object(ROOT, format!("object-{}", a % 4), object_type)
                .unwrap();
        }
        _ => unreachable!(),
    }

    if document.pending_ops() > 0 {
        let mut options = CommitOptions::default().with_time(i64::from(i16::from_le_bytes([c, d])));
        if a.is_multiple_of(2) {
            options = options.with_message(format!("private-message-{a}-{b}"));
        }
        document.commit_with(options);
    }
}

fn scalar(kind: u8, a: u8, b: u8) -> ScalarValue {
    match kind % 10 {
        0 => ScalarValue::from(format!("private-{a}-{b}")),
        1 => ScalarValue::Bytes(vec![a, b, kind]),
        2 => ScalarValue::Int(i64::from(i16::from_le_bytes([a, b]))),
        3 => ScalarValue::Uint(u64::from(u16::from_le_bytes([a, b]))),
        4 => ScalarValue::F64(f64::from(i16::from_le_bytes([a, b])) / 10.0),
        5 => ScalarValue::counter(i64::from(i16::from_le_bytes([a, b]))),
        6 => ScalarValue::Timestamp(i64::from(i16::from_le_bytes([a, b]))),
        7 => ScalarValue::Boolean(a.is_multiple_of(2)),
        8 => ScalarValue::Unknown {
            type_code: 10 + a % 6,
            bytes: vec![a, b],
        },
        _ => ScalarValue::Null,
    }
}

fn actor(index: u8) -> ActorId {
    ActorId::from(vec![index])
}
