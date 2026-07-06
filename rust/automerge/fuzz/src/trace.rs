use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Trace {
    pub version: u32,
    #[serde(default)]
    pub metadata: Metadata,
    #[serde(default)]
    pub actors: Vec<ActorSpec>,
    pub steps: Vec<VmInstr>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Metadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ActorSpec {
    pub bytes: Vec<u8>,
}

impl ActorSpec {
    pub fn new(index: usize) -> Self {
        Self {
            bytes: vec![index as u8],
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VmInstr {
    Fork {
        from: u8,
        to: u8,
    },
    Merge {
        into: u8,
        from: u8,
    },
    Change {
        doc: u8,
        actor: u8,
        ops: Vec<VmOp>,
    },
    SaveLoad {
        doc: u8,
    },
    Observe {
        doc: u8,
        #[serde(default = "default_observe_object")]
        object: VmObjRef,
        #[serde(default = "default_observe_mode")]
        mode: VmObserveMode,
        #[serde(default = "default_observe_head")]
        head: VmHeadRef,
        #[serde(default = "default_observe_budget")]
        budget: u8,
    },
    SaveHeads {
        doc: u8,
        slot: u8,
    },
    DiffRange {
        doc: u8,
        before: VmHeadRef,
        after: VmHeadRef,
    },
    UpdateDiffCursor {
        doc: u8,
    },
    ResetDiffCursor {
        doc: u8,
    },
    DiffIncremental {
        doc: u8,
    },
    Sync {
        left: u8,
        right: u8,
        rounds: u8,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VmOp {
    Put {
        obj: VmObjRef,
        key: u8,
        value: VmValue,
    },
    MakeMap {
        obj: VmObjRef,
        key: u8,
    },
    MakeList {
        obj: VmObjRef,
        key: u8,
    },
    MakeText {
        obj: VmObjRef,
        key: u8,
    },
    Insert {
        obj: VmObjRef,
        index: u8,
        value: VmValue,
    },
    PutSeq {
        obj: VmObjRef,
        index: u8,
        value: VmValue,
    },
    SpliceList {
        obj: VmObjRef,
        index: u8,
        delete: u8,
        values: Vec<VmValue>,
    },
    SpliceText {
        obj: VmObjRef,
        index: u8,
        delete: u8,
        value: u8,
    },
    UpdateText {
        obj: VmObjRef,
        value: u8,
    },
    Increment {
        obj: VmObjRef,
        key: u8,
        value: i8,
    },
    Mark {
        obj: VmObjRef,
        start: u8,
        end: u8,
        name: u8,
        value: VmValue,
        expand: MarkExpand,
    },
    Unmark {
        obj: VmObjRef,
        start: u8,
        end: u8,
        name: u8,
        expand: MarkExpand,
    },
    Delete {
        obj: VmObjRef,
        key: u8,
    },
    DeleteSeq {
        obj: VmObjRef,
        index: u8,
    },
    UpdateObject {
        obj: VmObjRef,
        value: VmHydrated,
    },
    BatchCreate {
        obj: VmObjRef,
        key: u8,
        value: VmHydrated,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VmObjRef {
    Root,
    Slot { slot: u8 },
    Recent { back: u8 },
    Invalid { slot: u8 },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VmHeadRef {
    Empty,
    Current,
    Slot { slot: u8 },
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VmObserveMode {
    Shallow,
    Hydrate,
    Ranges,
    Historical,
    MapGets,
    Text,
    Cursors,
    Marks,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VmValue {
    Null,
    Bool { slot: u8 },
    Int { slot: u8 },
    Uint { slot: u8 },
    Str { slot: u8 },
    Counter { slot: u8 },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VmHydrated {
    Scalar { value: VmValue },
    Map { seed: u8, depth: u8 },
    List { seed: u8, depth: u8 },
    Text { slot: u8 },
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarkExpand {
    Before,
    After,
    Both,
    None,
}

fn default_observe_object() -> VmObjRef {
    VmObjRef::Root
}

fn default_observe_mode() -> VmObserveMode {
    VmObserveMode::Shallow
}

fn default_observe_head() -> VmHeadRef {
    VmHeadRef::Current
}

fn default_observe_budget() -> u8 {
    4
}

impl Trace {
    pub fn generate(seed: u64, steps: usize) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut builder = VmSeedBuilder::default();
        let mut instructions = Vec::new();

        // Give generated traces a small amount of useful initial structure, but
        // keep it in the same VM representation as mutated traces.
        instructions.push(VmInstr::Change {
            doc: 0,
            actor: 0,
            ops: vec![
                VmOp::Put {
                    obj: VmObjRef::Root,
                    key: 0,
                    value: VmValue::Uint { slot: seed as u8 },
                },
                builder.make_list(1),
                builder.make_text(2),
            ],
        });

        for _ in 0..steps {
            instructions.push(builder.random_instr(&mut rng));
            if rng.random_range(0..100) < 15 {
                instructions.push(VmInstr::SaveLoad { doc: 0 });
            }
        }
        instructions.push(VmInstr::SaveLoad { doc: 0 });

        Self {
            version: 1,
            metadata: Metadata {
                seed: Some(seed),
                parent: None,
                reason: Some("generated".to_string()),
            },
            actors: vec![ActorSpec::new(0), ActorSpec::new(1), ActorSpec::new(2)],
            steps: instructions,
        }
    }
}

#[derive(Default)]
struct VmSeedBuilder {
    objects: u8,
    lists: Vec<u8>,
    texts: Vec<u8>,
}

impl VmSeedBuilder {
    fn make_list(&mut self, key: u8) -> VmOp {
        let slot = self.objects;
        self.objects = self.objects.saturating_add(1);
        self.lists.push(slot);
        VmOp::MakeList {
            obj: VmObjRef::Root,
            key,
        }
    }

    fn make_text(&mut self, key: u8) -> VmOp {
        let slot = self.objects;
        self.objects = self.objects.saturating_add(1);
        self.texts.push(slot);
        VmOp::MakeText {
            obj: VmObjRef::Root,
            key,
        }
    }

    fn random_instr(&mut self, rng: &mut StdRng) -> VmInstr {
        match rng.random_range(0..8) {
            0 => VmInstr::Fork { from: 0, to: 1 },
            1 => VmInstr::Merge { into: 0, from: 1 },
            2 => VmInstr::Observe {
                doc: 0,
                object: self.random_obj_ref(rng),
                mode: random_observe_mode(rng),
                head: VmHeadRef::Current,
                budget: rng.random_range(1..=8),
            },
            3 => VmInstr::UpdateDiffCursor { doc: 0 },
            4 => VmInstr::DiffIncremental { doc: 0 },
            5 => VmInstr::Sync {
                left: 0,
                right: 1,
                rounds: 8,
            },
            _ => VmInstr::Change {
                doc: 0,
                actor: rng.random_range(0..3),
                ops: vec![self.random_op(rng)],
            },
        }
    }

    fn random_obj_ref(&self, rng: &mut StdRng) -> VmObjRef {
        if self.objects == 0 || rng.random_range(0..100) < 60 {
            VmObjRef::Root
        } else {
            VmObjRef::Slot {
                slot: rng.random_range(0..self.objects),
            }
        }
    }

    fn random_op(&mut self, rng: &mut StdRng) -> VmOp {
        match rng.random_range(0..12) {
            0 => VmOp::Put {
                obj: VmObjRef::Root,
                key: rng.random(),
                value: random_value(rng),
            },
            1 => self.make_list(rng.random()),
            2 => self.make_text(rng.random()),
            3 if !self.lists.is_empty() => VmOp::Insert {
                obj: VmObjRef::Slot {
                    slot: self.lists[rng.random_range(0..self.lists.len())],
                },
                index: rng.random(),
                value: random_value(rng),
            },
            4..=5 if !self.texts.is_empty() => VmOp::SpliceText {
                obj: VmObjRef::Slot {
                    slot: self.texts[rng.random_range(0..self.texts.len())],
                },
                index: rng.random(),
                delete: rng.random_range(0..4),
                value: rng.random(),
            },
            6 if !self.texts.is_empty() => VmOp::Mark {
                obj: VmObjRef::Slot {
                    slot: self.texts[rng.random_range(0..self.texts.len())],
                },
                start: rng.random(),
                end: rng.random(),
                name: rng.random(),
                value: random_value(rng),
                expand: random_mark_expand(rng),
            },
            _ => VmOp::BatchCreate {
                obj: VmObjRef::Root,
                key: rng.random(),
                value: VmHydrated::Map {
                    seed: rng.random(),
                    depth: rng.random_range(1..=3),
                },
            },
        }
    }
}

fn random_value(rng: &mut StdRng) -> VmValue {
    match rng.random_range(0..6) {
        0 => VmValue::Null,
        1 => VmValue::Bool { slot: rng.random() },
        2 => VmValue::Int { slot: rng.random() },
        3 => VmValue::Uint { slot: rng.random() },
        4 => VmValue::Counter { slot: rng.random() },
        _ => VmValue::Str { slot: rng.random() },
    }
}

fn random_observe_mode(rng: &mut StdRng) -> VmObserveMode {
    match rng.random_range(0..8) {
        0 => VmObserveMode::Shallow,
        1 => VmObserveMode::Hydrate,
        2 => VmObserveMode::Ranges,
        3 => VmObserveMode::Historical,
        4 => VmObserveMode::MapGets,
        5 => VmObserveMode::Text,
        6 => VmObserveMode::Cursors,
        _ => VmObserveMode::Marks,
    }
}

fn random_mark_expand(rng: &mut StdRng) -> MarkExpand {
    match rng.random_range(0..4) {
        0 => MarkExpand::Before,
        1 => MarkExpand::After,
        2 => MarkExpand::Both,
        _ => MarkExpand::None,
    }
}
