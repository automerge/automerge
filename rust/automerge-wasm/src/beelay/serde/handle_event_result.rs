use beelay_core::{CommitOrBundle, DocEvent, Envelope};
use js_sys::{Array, Object, Reflect, Uint8Array};
use wasm_bindgen::JsValue;

pub(crate) fn serialize_event_results(result: beelay_core::EventResults) -> JsValue {
    let obj = Object::new();

    let messages = Array::new();
    for message in result.new_messages {
        messages.push(&serialize_envelope(message));
    }
    set_field(&obj, "new_messages", messages.into());

    let doc_events = Array::new();
    for doc_evt in result.notifications {
        doc_events.push(&serialize_docevent(doc_evt));
    }
    set_field(&obj, "notifications", doc_events.into());

    let tasks = Array::new();
    for task in result.new_tasks {
        tasks.push(&serialize_task(task));
    }
    set_field(&obj, "new_tasks", tasks.into());

    let completed_stories = Object::new();
    for (story_id, story_result) in result.completed_stories {
        let story_result = serialize_story_result(story_id, story_result);
        set_field(&completed_stories, &story_id.serialize(), story_result);
    }
    set_field(&obj, "completed_stories", completed_stories.into());

    obj.into()
}

fn set_field(obj: &Object, key: &str, value: JsValue) {
    Reflect::set(obj.as_ref(), &JsValue::from_str(key), &value).unwrap();
}

fn serialize_envelope(env: Envelope) -> JsValue {
    let result = Object::new();

    set_field(
        &result,
        "sender",
        JsValue::from_str(&env.sender().to_string()),
    );
    set_field(
        &result,
        "recipient",
        JsValue::from_str(&env.recipient().to_string()),
    );
    let encoded_payload = Uint8Array::from(env.payload().encode().as_slice());
    set_field(&result, "message", encoded_payload.into());

    result.into()
}

fn serialize_task(task: beelay_core::io::IoTask) -> JsValue {
    let result = Object::new();

    set_field(&result, "id", task.id().serialize().into());
    match task.action() {
        beelay_core::io::IoAction::Load { key } => {
            let key = serialize_key(key);
            set_field(&result, "action", JsValue::from_str("load"));
            set_field(&result, "key", key);
        }
        beelay_core::io::IoAction::LoadRange { prefix } => {
            let prefix = serialize_key(prefix);
            set_field(&result, "action", JsValue::from_str("load_range"));
            set_field(&result, "prefix", prefix);
        }
        beelay_core::io::IoAction::Put { key, data } => {
            let key = serialize_key(key);
            set_field(&result, "action", JsValue::from_str("put"));
            set_field(&result, "key", key);
            let data = Uint8Array::from(data.as_slice());
            set_field(&result, "data", JsValue::from(data));
        }
        beelay_core::io::IoAction::Delete { key } => {
            let key = serialize_key(key);
            set_field(&result, "action", JsValue::from_str("delete"));
            set_field(&result, "key", key);
        }
    }

    result.into()
}

fn serialize_story_result(
    story_id: beelay_core::StoryId,
    story_result: beelay_core::StoryResult,
) -> JsValue {
    let result = Object::new();
    set_field(&result, "story_id", story_id.serialize().into());

    match story_result {
        beelay_core::StoryResult::AddCommits(new_bundles_required) => {
            set_field(&result, "story_type", JsValue::from_str("add_commits"));
            let bundles = Array::new();
            for bundle in new_bundles_required {
                let bundle_obj = Object::new();
                set_field(
                    &bundle_obj,
                    "start",
                    JsValue::from_str(&bundle.start.to_string()),
                );
                set_field(
                    &bundle_obj,
                    "end",
                    JsValue::from_str(&bundle.end.to_string()),
                );
                let checkpoints = Array::new();
                for checkpoint in bundle.checkpoints {
                    checkpoints.push(&JsValue::from_str(&checkpoint.to_string()));
                }
                set_field(&bundle_obj, "checkpoints", checkpoints.into());
                bundles.push(&bundle_obj);
            }
            set_field(&result, "new_bundles_required", bundles.into());
        }
        beelay_core::StoryResult::SyncCollection(docs) => {
            set_field(&result, "story_type", JsValue::from_str("sync_collection"));
            let js_docs = Array::new();
            for doc in docs {
                js_docs.push(&JsValue::from_str(&doc.to_string()));
            }
            set_field(&result, "documents", js_docs.into());
        }
        beelay_core::StoryResult::CreateDoc(doc_id) => {
            set_field(&result, "story_type", JsValue::from_str("create_document"));
            set_field(&result, "document_id", doc_id.to_string().into());
        }
        beelay_core::StoryResult::LoadDoc(commits) => {
            set_field(&result, "story_type", JsValue::from_str("load_document"));
            if let Some(commits) = commits {
                set_field(&result, "commits", serialize_commits(commits));
            }
        }
        beelay_core::StoryResult::AddLink => {
            set_field(&result, "story_type", JsValue::from_str("add_link"));
        }
        beelay_core::StoryResult::AddBundle => {
            set_field(&result, "story_type", JsValue::from_str("add_bundle"));
        }
    }

    result.into()
}

fn serialize_key(key: &beelay_core::StorageKey) -> JsValue {
    let result = Array::new();
    for part in key.components() {
        result.push(&JsValue::from_str(part));
    }
    result.into()
}

fn serialize_commits(commits: Vec<CommitOrBundle>) -> JsValue {
    let result = Array::new();
    for item in commits {
        let commit_obj = serialize_commit_or_bundle(item);
        result.push(&commit_obj);
    }
    result.into()
}

fn serialize_docevent(evt: DocEvent) -> JsValue {
    let event = Object::new();
    set_field(&event, "docId", evt.doc.to_string().into());

    let obj = serialize_commit_or_bundle(evt.data);
    set_field(&event, "data", obj);

    event.into()
}

fn serialize_commit_or_bundle(c_or_b: CommitOrBundle) -> JsValue {
    let commit_obj = Object::new();
    match c_or_b {
        CommitOrBundle::Commit(commit) => {
            set_field(&commit_obj, "type", JsValue::from_str("commit"));
            set_field(
                &commit_obj,
                "hash",
                JsValue::from_str(&commit.hash().to_string()),
            );

            let parents = Array::new();
            for parent in commit.parents() {
                parents.push(&JsValue::from_str(&parent.to_string()));
            }
            set_field(&commit_obj, "parents", parents.into());

            let contents = Uint8Array::from(commit.contents());
            set_field(&commit_obj, "contents", JsValue::from(contents));
        }
        CommitOrBundle::Bundle(bundle) => {
            set_field(&commit_obj, "type", JsValue::from_str("bundle"));
            set_field(
                &commit_obj,
                "start",
                JsValue::from_str(bundle.start().to_string().as_str()),
            );
            set_field(
                &commit_obj,
                "end",
                JsValue::from_str(bundle.end().to_string().as_str()),
            );
            let checkpoints = Array::new();
            for checkpoint in bundle.checkpoints() {
                checkpoints.push(&JsValue::from_str(&checkpoint.to_string()));
            }
            set_field(&commit_obj, "checkpoints", checkpoints.into());
        }
    };
    commit_obj.into()
}
