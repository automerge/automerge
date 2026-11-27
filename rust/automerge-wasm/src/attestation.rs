//! JS conversions for the attested signing surface.
//!
//! `insertSignatures` parses the complete input batch into owned
//! [`SignatureInput`]s before the document is touched, so a malformed entry
//! anywhere in the batch rejects the whole call without processing earlier
//! valid entries or committing the pending transaction. Successful parses are
//! handed to [`automerge::AutoCommit::insert_signatures`], whose structured
//! outcome is mirrored back as a camelCase discriminated union.

use automerge as am;
use automerge::signature::ed25519;
use automerge::signature::{Insert, SIGNING_MESSAGE_LEN};
use automerge::{ChangeHash, SignatureBatchOutcome, SignatureInsertOutcome, SigningRequest};
use js_sys::{Array, Object, Uint8Array};
use wasm_bindgen::{JsCast, JsValue};

use crate::interop::{js_get, js_set};

/// One parsed `{message, signature}` entry with its owned bytes.
pub(crate) struct SignatureInput {
    hash: ChangeHash,
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
    // Lengths are checked on the JS views before any bytes are copied, so a
    // malformed oversized buffer costs nothing to reject.
    let message = sized_copy(&message, SIGNING_MESSAGE_LEN).map_err(|actual| {
        BadInput::Message(am::signature::error::SigningMessage::InvalidHashLength {
            actual: actual.saturating_sub(SIGNING_PREFIX_LEN),
        })
    })?;
    let signature = sized_copy(&signature, 64).map_err(BadInput::SignatureLength)?;
    let hash = am::signature::parse_signing_message(&message)?;
    let signature: [u8; 64] = signature.try_into().expect("length checked before copying");
    Ok(SignatureInput {
        hash,
        signature: ed25519::Signature::from_bytes(signature),
    })
}

const SIGNING_PREFIX_LEN: usize = SIGNING_MESSAGE_LEN - 32;

fn bytes_prop(entry: &JsValue, name: &str) -> Option<Uint8Array> {
    let value = js_get(entry, name).ok()?.0;
    value.dyn_into::<Uint8Array>().ok()
}

/// Copy `bytes` only if the view holds exactly `expected` bytes, otherwise
/// return the actual length.
fn sized_copy(bytes: &Uint8Array, expected: usize) -> Result<Vec<u8>, usize> {
    let actual = bytes.length() as usize;
    if actual != expected {
        return Err(actual);
    }
    Ok(bytes.to_vec())
}

/// `{status: "rejected", reason}` for a malformed batch.
pub(crate) fn rejected_to_js(error: &dyn std::fmt::Display) -> JsValue {
    let obj: JsValue = Object::new().into();
    js_set(&obj, "status", "rejected").unwrap();
    js_set(&obj, "reason", error.to_string()).unwrap();
    obj
}

/// Convert each owned native outcome without relying on input/result ordering.
pub(crate) fn outcome_to_js(outcome: SignatureBatchOutcome) -> JsValue {
    match outcome {
        SignatureBatchOutcome::Rejected { error } => rejected_to_js(&error),
        SignatureBatchOutcome::Completed { insertions } => {
            let obj: JsValue = Object::new().into();
            js_set(&obj, "status", "completed").unwrap();
            js_set(&obj, "insertions", insertions_to_js(&insertions)).unwrap();
            obj
        }
        SignatureBatchOutcome::PublicationFailed { insertions, error } => {
            let obj: JsValue = Object::new().into();
            js_set(&obj, "status", "publicationFailed").unwrap();
            js_set(&obj, "insertions", insertions_to_js(&insertions)).unwrap();
            js_set(&obj, "error", error.to_string()).unwrap();
            obj
        }
    }
}

fn insertions_to_js(insertions: &[SignatureInsertOutcome]) -> Array {
    insertions.iter().map(insertion_to_js).collect()
}

fn insertion_to_js(insertion: &SignatureInsertOutcome) -> JsValue {
    let obj: JsValue = Object::new().into();
    js_set(&obj, "hash", insertion.hash.to_string()).unwrap();
    js_set(
        &obj,
        "message",
        Uint8Array::from(am::signature::signing_message(&insertion.hash).as_slice()),
    )
    .unwrap();
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
        Insert::DroppedOverflow => "droppedOverflow",
    }
}

/// `{hash, message}` with an owned copy of the message bytes.
pub(crate) fn request_to_js(request: &SigningRequest) -> JsValue {
    let obj: JsValue = Object::new().into();
    js_set(&obj, "hash", request.hash().to_string()).unwrap();
    js_set(&obj, "message", Uint8Array::from(request.message())).unwrap();
    obj
}
