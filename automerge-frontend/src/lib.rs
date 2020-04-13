use automerge_backend::{Patch, ActorID, RequestKey};

mod value;
mod change_context;
mod error;

pub use value::{Value, SequenceType, MapType};
pub use error::AutomergeFrontendError;


struct Frontend {
    actor_id: ActorID,
}

impl Frontend {
    fn new() -> Self {
        return Frontend{
            actor_id: ActorID::random()
        }
    }

    fn new_with_initial_state(inital_state: Value) -> Self {
        panic!("not implemented")
    }

    fn state(&self) -> Value {
        Value::Null
    }

    fn apply_patch(&mut self, patch: Patch) -> () {
    }
}

enum PathElement {
    Key(String),
    Index(usize),
}

impl PathElement {
    pub(crate) fn to_request_key(&self) -> RequestKey {
        match self {
            PathElement::Key(s) => RequestKey::Str(s.into()),
            PathElement::Index(i) => RequestKey::Num(*i as u64),
        }
    }
}

enum LocalOperation {
    Set(Value),
    Delete,
}

struct LocalChange {
    path: Vec<PathElement>,
    operation: LocalOperation
}

#[cfg(test)]
mod tests {

}
