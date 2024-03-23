use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use crate::exid::ExId;
use crate::types::Prop;
use crate::{PatchAction, ScalarValue};

use super::{HydrateError, Value};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Map(HashMap<String, MapValue>);

#[derive(Clone, Debug, PartialEq)]
pub struct MapValue {
    pub value: Value,
    pub conflict: bool,
}

impl Map {
    pub fn iter(&self) -> impl Iterator<Item = (&String, &MapValue)> {
        self.0.iter()
    }

    pub(crate) fn apply(&mut self, patch: PatchAction) -> Result<(), HydrateError> {
        match patch {
            PatchAction::DeleteMap { key } => {
                self.0.remove(&key);
                Ok(())
            }
            PatchAction::PutMap {
                key,
                value,
                conflict,
            } => {
                self.0
                    .insert(key, MapValue::new(value.0.into(), value.1, conflict));
                Ok(())
            }
            PatchAction::Increment {
                prop: Prop::Map(key),
                value,
            } => {
                self.0
                    .get_mut(&key)
                    .ok_or(HydrateError::InvalidKey(key))?
                    .increment(value)?;
                Ok(())
            }
            _ => Err(HydrateError::InvalidMapOp),
        }
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key).map(|mv| &mv.value)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
        self.0.get_mut(key).map(|mv| &mut mv.value)
    }

    pub(crate) fn new() -> Self {
        Self(Default::default())
    }
}

impl MapValue {
    pub(crate) fn new(value: Value, _id: ExId, conflict: bool) -> Self {
        Self { value, conflict }
    }

    pub(crate) fn increment(&mut self, n: i64) -> Result<(), HydrateError> {
        if let Value::Scalar(ScalarValue::Counter(c)) = &mut self.value {
            c.increment(n);
            Ok(())
        } else {
            Err(HydrateError::BadIncrement)
        }
    }
}

impl Deref for Map {
    type Target = HashMap<String, MapValue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Map {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<HashMap<&str, Value>> for Map {
    fn from(value: HashMap<&str, Value>) -> Self {
        Map(value
            .into_iter()
            .map(|(k, value)| {
                (
                    k.to_string(),
                    MapValue {
                        value,
                        conflict: false,
                    },
                )
            })
            .collect())
    }
}

impl From<HashMap<String, Value>> for Map {
    fn from(value: HashMap<String, Value>) -> Self {
        Map(value
            .into_iter()
            .map(|(k, value)| {
                (
                    k,
                    MapValue {
                        value,
                        conflict: false,
                    },
                )
            })
            .collect())
    }
}
