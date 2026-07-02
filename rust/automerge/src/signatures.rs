use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use crate::{Author, ChangeHash, ChangeSignature};

const SIGNING_DOMAIN: &[u8] = b"automerge-change-signature-v1";

pub(crate) fn signing_payload(hash: ChangeHash) -> Vec<u8> {
    let mut payload = Vec::with_capacity(SIGNING_DOMAIN.len() + hash.as_bytes().len());
    payload.extend_from_slice(SIGNING_DOMAIN);
    payload.extend_from_slice(hash.as_bytes());
    payload
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SignatureAlgorithm {
    Ed25519,
    Other(String),
}

impl Default for SignatureAlgorithm {
    fn default() -> Self {
        Self::Ed25519
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct VerificationRequestId(u64);

impl VerificationRequestId {
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for VerificationRequestId {
    fn from(value: u64) -> Self {
        VerificationRequestId(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SigningRequest {
    hash: ChangeHash,
    author: Author,
    bytes_to_sign: Vec<u8>,
    started: bool,
}

impl SigningRequest {
    pub(crate) fn new(hash: ChangeHash, author: Author) -> Self {
        Self {
            hash,
            author,
            bytes_to_sign: signing_payload(hash),
            started: false,
        }
    }

    pub fn hash(&self) -> ChangeHash {
        self.hash
    }

    pub fn author(&self) -> &Author {
        &self.author
    }

    pub fn bytes_to_sign(&self) -> &[u8] {
        &self.bytes_to_sign
    }

    pub fn is_started(&self) -> bool {
        self.started
    }

    pub fn mark_started(&mut self) {
        self.started = true;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerificationRequest {
    id: VerificationRequestId,
    hash: ChangeHash,
    author: Author,
    algorithm: SignatureAlgorithm,
    signature: Option<ChangeSignature>,
    bytes_to_verify: Vec<u8>,
    started: bool,
}

impl VerificationRequest {
    pub(crate) fn new(
        id: VerificationRequestId,
        hash: ChangeHash,
        author: Author,
        signature: Option<ChangeSignature>,
    ) -> Self {
        Self {
            id,
            hash,
            author,
            algorithm: SignatureAlgorithm::Ed25519,
            signature,
            bytes_to_verify: signing_payload(hash),
            started: false,
        }
    }

    pub fn id(&self) -> VerificationRequestId {
        self.id.clone()
    }

    pub fn hash(&self) -> ChangeHash {
        self.hash
    }

    pub fn author(&self) -> &Author {
        &self.author
    }

    pub fn algorithm(&self) -> &SignatureAlgorithm {
        &self.algorithm
    }

    pub fn signature(&self) -> Option<&ChangeSignature> {
        self.signature.as_ref()
    }

    pub fn bytes_to_verify(&self) -> &[u8] {
        &self.bytes_to_verify
    }

    pub fn is_started(&self) -> bool {
        self.started
    }

    pub fn mark_started(&mut self) {
        self.started = true;
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SignatureReport {
    pub signing_requested: usize,
    pub signatures_attached: usize,
    pub verification_requested: usize,
    pub verification_accepted: usize,
    pub verification_rejected: usize,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SignatureError {
    #[error("signature is required for {hash}")]
    MissingSignature { hash: ChangeHash },
    #[error("{missing} required signatures are missing")]
    SigningIncomplete { missing: usize },
    #[error("signature verification failed for {hash}")]
    VerificationFailed { hash: ChangeHash },
}

#[derive(Clone, Debug, Default)]
pub struct SignatureState {
    signing: HashMap<ChangeHash, SigningRequest>,
    completed_signatures: HashMap<ChangeHash, ChangeSignature>,
    attached: HashSet<ChangeHash>,
    verification: HashMap<VerificationRequestId, VerificationRequest>,
    completed_verification: HashMap<VerificationRequestId, bool>,
    next_verification_id: u64,
}

impl SignatureState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn pending_signing_requests(&self) -> impl Iterator<Item = &SigningRequest> {
        self.signing
            .iter()
            .filter(|(hash, _)| {
                !self.completed_signatures.contains_key(hash) && !self.attached.contains(hash)
            })
            .map(|(_, request)| request)
    }

    pub fn pending_signing_requests_mut(&mut self) -> impl Iterator<Item = &mut SigningRequest> {
        let completed = &self.completed_signatures;
        let attached = &self.attached;
        self.signing
            .iter_mut()
            .filter(move |(hash, _)| !completed.contains_key(hash) && !attached.contains(hash))
            .map(|(_, request)| request)
    }

    pub fn ensure_signing_request(&mut self, hash: ChangeHash, author: Author) -> bool {
        if self.attached.contains(&hash) || self.completed_signatures.contains_key(&hash) {
            return false;
        }
        match self.signing.entry(hash) {
            std::collections::hash_map::Entry::Occupied(_) => false,
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(SigningRequest::new(hash, author));
                true
            }
        }
    }

    pub fn mark_signing_started(&mut self, hash: &ChangeHash) -> bool {
        if let Some(request) = self.signing.get_mut(hash) {
            request.mark_started();
            true
        } else {
            false
        }
    }

    pub fn complete_signing<S: Into<ChangeSignature>>(&mut self, hash: ChangeHash, signature: S) {
        self.completed_signatures.insert(hash, signature.into());
    }

    pub(crate) fn take_completed_signature(
        &mut self,
        hash: &ChangeHash,
    ) -> Option<ChangeSignature> {
        self.completed_signatures.remove(hash)
    }

    pub(crate) fn mark_attached(&mut self, hash: ChangeHash) {
        self.attached.insert(hash);
        self.signing.remove(&hash);
    }

    pub fn ensure_verification_request(
        &mut self,
        hash: ChangeHash,
        author: Author,
        signature: Option<ChangeSignature>,
    ) -> VerificationRequestId {
        if let Some((id, _)) = self.verification.iter().find(|(_, request)| {
            request.hash == hash && request.author == author && request.signature == signature
        }) {
            return id.clone();
        }
        let id = VerificationRequestId(self.next_verification_id);
        self.next_verification_id += 1;
        self.verification.insert(
            id.clone(),
            VerificationRequest::new(id.clone(), hash, author, signature),
        );
        id
    }

    pub fn pending_verification_requests(&self) -> impl Iterator<Item = &VerificationRequest> {
        self.verification
            .iter()
            .filter(|(id, _)| !self.completed_verification.contains_key(id))
            .map(|(_, request)| request)
    }

    pub fn pending_verification_requests_mut(
        &mut self,
    ) -> impl Iterator<Item = &mut VerificationRequest> {
        let completed = &self.completed_verification;
        self.verification
            .iter_mut()
            .filter(move |(id, _)| !completed.contains_key(id))
            .map(|(_, request)| request)
    }

    pub fn mark_verification_started(&mut self, id: &VerificationRequestId) -> bool {
        if let Some(request) = self.verification.get_mut(id) {
            request.mark_started();
            true
        } else {
            false
        }
    }

    pub fn complete_verification(&mut self, id: VerificationRequestId, valid: bool) {
        self.completed_verification.insert(id, valid);
    }

    pub fn verification_result(&self, id: &VerificationRequestId) -> Option<bool> {
        self.completed_verification.get(id).copied()
    }

    pub(crate) fn take_verification_result(&mut self, id: &VerificationRequestId) -> Option<bool> {
        self.completed_verification.remove(id)
    }

    pub(crate) fn remove_verification_request(&mut self, id: &VerificationRequestId) {
        self.verification.remove(id);
        self.completed_verification.remove(id);
    }

    pub fn clear_attached(&mut self) {
        self.attached.clear();
    }
}

impl<'a> From<&'a SigningRequest> for Cow<'a, [u8]> {
    fn from(value: &'a SigningRequest) -> Self {
        Cow::Borrowed(value.bytes_to_sign())
    }
}
