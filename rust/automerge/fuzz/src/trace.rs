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

#[derive(Clone, Debug, Hash, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, Debug, Hash, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, Debug, Hash, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VmObjRef {
    Root,
    Slot { slot: u8 },
    Recent { back: u8 },
    Invalid { slot: u8 },
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VmHeadRef {
    Empty,
    Current,
    Slot { slot: u8 },
}

#[derive(Clone, Copy, Debug, Hash, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VmValue {
    Null,
    Bool { slot: u8 },
    Int { slot: u8 },
    Uint { slot: u8 },
    Str { slot: u8 },
    Counter { slot: u8 },
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VmHydrated {
    Scalar { value: VmValue },
    Map { seed: u8, depth: u8 },
    List { seed: u8, depth: u8 },
    Text { slot: u8 },
}

#[derive(Clone, Copy, Debug, Hash, serde::Serialize, serde::Deserialize)]
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
