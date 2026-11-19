//! JS conversions for the required-mode signing surface.
//!
//! `insertSignatures` parses the complete input batch into owned
//! [`SignatureInput`]s before the document is touched, so a malformed entry
//! anywhere in the batch rejects the whole call without processing earlier
//! valid entries or committing the pending transaction. Successful parses are
//! handed to [`automerge::AutoCommit::insert_signatures`], whose structured
//! outcome is mirrored back as a camelCase discriminated union.

use automerge as am;
use automerge::signature::ed25519;
use automerge::signature::Insert;
use automerge::{ChangeHash, SignatureBatchOutcome, SignatureInsertOutcome, SigningRequest};
use js_sys::{Array, Object, Uint8Array};
use wasm_bindgen::{JsCast, JsValue};

use crate::interop::{js_get, js_set};

/// One parsed `{message, signature}` entry with its owned bytes.
pub(crate) struct SignatureInput {
    hash: ChangeHash,
    message: Vec<u8>,
    signature: ed25519::Signature,
}

impl SignatureInput {
    /// The pair handed to the native batch receipt.
    pub(crate) fn pair(&self) -> (ChangeHash, ed25519::Signature) {
        (self.hash, self.signature)
    }
}

/// Why a batch could not be parsed; nothing was processed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BadBatch {
    #[error("signature inputs must be an array")]
    NotArray,
    #[error("bad signature input at index {0}: {1}")]
    BadElem(usize, BadInput),
}

/// Why one entry could not be parsed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BadInput {
    #[error("entry must be an object with message and signature")]
    NotObject,
    #[error("message must be a Uint8Array")]
    MessageNotBytes,
    #[error("signature must be a Uint8Array")]
    SignatureNotBytes,
    #[error("bad signing message: {0}")]
    Message(#[from] am::signature::error::SigningMessage),
    #[error("ed25519 signature must contain 64 bytes, found {0}")]
    SignatureLength(usize),
}

/// Parse every entry of `inputs`, failing on the first malformed one.
pub(crate) fn parse_batch(inputs: &JsValue) -> Result<Vec<SignatureInput>, BadBatch> {
    let array = inputs.dyn_ref::<Array>().ok_or(BadBatch::NotArray)?;
    array
        .iter()
        .enumerate()
        .map(|(index, entry)| parse_input(&entry).map_err(|e| BadBatch::BadElem(index, e)))
        .collect()
}

fn parse_input(entry: &JsValue) -> Result<SignatureInput, BadInput> {
    if !entry.is_object() {
        return Err(BadInput::NotObject);
    }
    let message = bytes_prop(entry, "message").ok_or(BadInput::MessageNotBytes)?;
    let signature = bytes_prop(entry, "signature").ok_or(BadInput::SignatureNotBytes)?;
    let hash = am::signature::parse_signing_message(&message)?;
    let signature: [u8; 64] = signature
        .try_into()
        .map_err(|bytes: Vec<u8>| BadInput::SignatureLength(bytes.len()))?;
    Ok(SignatureInput {
        hash,
        message,
        signature: ed25519::Signature::from_bytes(signature),
    })
}

fn bytes_prop(entry: &JsValue, name: &str) -> Option<Vec<u8>> {
    let value = js_get(entry, name).ok()?.0;
    Some(value.dyn_ref::<Uint8Array>()?.to_vec())
}

/// `{status: "rejected", reason}` for a malformed batch.
pub(crate) fn rejected_to_js(error: &dyn std::fmt::Display) -> JsValue {
    let obj: JsValue = Object::new().into();
    js_set(&obj, "status", "rejected").unwrap();
    js_set(&obj, "reason", error.to_string()).unwrap();
    obj
}

/// Mirror a native batch outcome. `inputs` supplies the owned message bytes
/// for each insertion, which the native outcome identifies by hash only.
pub(crate) fn outcome_to_js(inputs: &[SignatureInput], outcome: SignatureBatchOutcome) -> JsValue {
    match outcome {
        SignatureBatchOutcome::Rejected { error } => rejected_to_js(&error),
        SignatureBatchOutcome::Completed { insertions } => {
            let obj: JsValue = Object::new().into();
            js_set(&obj, "status", "completed").unwrap();
            js_set(&obj, "insertions", insertions_to_js(inputs, &insertions)).unwrap();
            obj
        }
        SignatureBatchOutcome::PublicationFailed { insertions, error } => {
            let obj: JsValue = Object::new().into();
            js_set(&obj, "status", "publicationFailed").unwrap();
            js_set(&obj, "insertions", insertions_to_js(inputs, &insertions)).unwrap();
            js_set(&obj, "error", error.to_string()).unwrap();
            obj
        }
    }
}

fn insertions_to_js(inputs: &[SignatureInput], insertions: &[SignatureInsertOutcome]) -> Array {
    debug_assert_eq!(inputs.len(), insertions.len());
    inputs
        .iter()
        .zip(insertions)
        .map(|(input, insertion)| insertion_to_js(input, insertion))
        .collect()
}

fn insertion_to_js(input: &SignatureInput, insertion: &SignatureInsertOutcome) -> JsValue {
    debug_assert_eq!(input.hash, insertion.hash);
    let obj: JsValue = Object::new().into();
    js_set(&obj, "hash", insertion.hash.to_string()).unwrap();
    js_set(&obj, "message", Uint8Array::from(input.message.as_slice())).unwrap();
    js_set(
        &obj,
        "signature",
        Uint8Array::from(insertion.signature.to_bytes().as_slice()),
    )
    .unwrap();
    let result: JsValue = Object::new().into();
    match &insertion.result {
        Ok(insert) => {
            js_set(&result, "ok", true).unwrap();
            js_set(&result, "insert", insert_name(*insert)).unwrap();
        }
        Err(error) => {
            js_set(&result, "ok", false).unwrap();
            js_set(&result, "error", error.to_string()).unwrap();
        }
    }
    js_set(&obj, "result", result).unwrap();
    obj
}

fn insert_name(insert: Insert) -> &'static str {
    match insert {
        Insert::Stored => "stored",
        Insert::Duplicate => "duplicate",
        Insert::EstablishedEd25519Evidence => "establishedEd25519Evidence",
        Insert::ReplacedUnchecked => "replacedUnchecked",
        Insert::DroppedOverflow => "droppedOverflow",
    }
}

/// `{hash, author, message}` with an owned copy of the message bytes.
pub(crate) fn request_to_js(request: &SigningRequest) -> JsValue {
    let obj: JsValue = Object::new().into();
    js_set(&obj, "hash", request.hash().to_string()).unwrap();
    js_set(&obj, "author", request.author().to_string()).unwrap();
    js_set(&obj, "message", Uint8Array::from(request.message())).unwrap();
    obj
}
