#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Trace {
    pub version: u32,
    #[serde(default)]
    pub metadata: Metadata,
    #[serde(default)]
    pub actors: Vec<ActorSpec>,
    /// Text encoding the documents are created (and reloaded) with. `None`
    /// means the default `UnicodeCodePoint`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_encoding: Option<VmTextEncoding>,
    pub steps: Vec<VmInstr>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VmTextEncoding {
    CodePoint,
    Utf8,
    Utf16,
    Grapheme,
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
    /// Fork `from` at a historical point (resolved via `head`) into slot `to`.
    ForkAt {
        from: u8,
        to: u8,
        head: VmHeadRef,
    },
    /// Transfer the changes `into` is missing from `from` via `apply_changes`,
    /// delivered in an adversarial order. Any complete delivery order must
    /// leave `into` containing all of `from`'s heads once the causal queue
    /// drains.
    ApplyChanges {
        from: u8,
        into: u8,
        order: VmApplyOrder,
    },
    Change {
        doc: u8,
        actor: u8,
        ops: Vec<VmOp>,
    },
    /// Apply `ops` inside an explicit `Automerge::transaction()` on a copy of
    /// the document. On `commit: true` the resulting changes are fed back into
    /// the live document through `apply_changes`; on `commit: false` the
    /// transaction is rolled back, which must leave the copy untouched.
    Transact {
        doc: u8,
        actor: u8,
        ops: Vec<VmOp>,
        commit: bool,
    },
    /// Run an owned transaction against a historical view of the document.
    TransactAt {
        doc: u8,
        actor: u8,
        head: VmHeadRef,
        ops: Vec<VmOp>,
        commit: bool,
    },
    /// Transfer encoded changes through one of Automerge's persistence APIs.
    Persist {
        from: u8,
        into: u8,
        mode: VmPersistMode,
    },
    /// Restrict an AutoCommit view to historical heads until `Integrate`.
    Isolate {
        doc: u8,
        head: VmHeadRef,
    },
    Integrate {
        doc: u8,
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
    /// One step of a persistent sync session. Sessions hold live
    /// `sync::State`s and an in-flight queue of encoded messages, so edits,
    /// save/loads, and message faults can be interleaved with the protocol.
    SyncSession {
        session: u8,
        op: VmSyncOp,
    },
}

#[derive(Clone, Debug, Hash, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VmSyncOp {
    /// Create (or replace) the session with fresh sync states between two docs.
    Start { left: u8, right: u8 },
    /// Generate one sync message from a side into the in-flight queue.
    Generate { from_left: bool },
    /// Deliver (or mis-deliver, per `fault`) one queued message to a side.
    Deliver { to_left: bool, fault: VmSyncFault },
    /// Encode/decode both sync states in place, as a process restart would.
    SaveStates,
    /// Toggle whether one side accepts incoming changes. Read-only peers still
    /// publish their own changes, which makes this a directional sync mode.
    SetReadOnly { left: bool, read_only: bool },
    /// Deliver everything in flight, then sync reliably for up to `rounds`
    /// rounds. Writable peers must converge; in directional read-only mode the
    /// writable peer must contain everything published by the read-only peer.
    Finish { rounds: u8 },
}

#[derive(Clone, Copy, Debug, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VmSyncFault {
    None,
    Drop,
    Duplicate,
    Reorder,
}

#[derive(Clone, Debug, Hash, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VmPersistMode {
    /// `save_incremental` followed by `load_incremental`.
    Incremental,
    /// `save_after` followed by `load_incremental`.
    SaveAfter { since: VmHeadRef },
    /// Bundle selected changes, round-trip the bundle codec, then load it.
    Bundle { since: VmHeadRef },
}

#[derive(Clone, Copy, Debug, Hash, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VmApplyOrder {
    InOrder,
    /// Children before parents: everything waits in the causal queue.
    Reversed,
    Shuffled {
        seed: u8,
    },
    /// Every change delivered twice.
    Duplicated,
    /// Every other change withheld, leaving the queue with pending changes.
    DropHalf,
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
    /// `update_text` with a small edit derived from the object's *current*
    /// text: near-identical before/after strings drive the Myers diff much
    /// deeper than swapping between canned strings does.
    EditText {
        obj: VmObjRef,
        seed: u8,
    },
    /// Replace a text object's rich content (text spans, marks, and block
    /// markers) via `update_spans`, which drives the block/marks diff path.
    UpdateSpans {
        obj: VmObjRef,
        seed: u8,
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
    Timestamp { slot: u8 },
    F64 { slot: u8 },
    Bytes { slot: u8 },
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
