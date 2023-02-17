use crate::value::ScalarValue;

#[derive(Debug, Clone, PartialEq)]
pub struct Mark {
    pub start: usize,
    pub end: usize,
    pub expand_left: bool,
    pub expand_right: bool,
    pub name: String,
    pub value: ScalarValue,
}

impl Default for Mark {
    fn default() -> Self {
        Mark {
            name: "".into(),
            value: ScalarValue::Null,
            start: 0,
            end: 0,
            expand_left: false,
            expand_right: false,
        }
    }
}

impl Mark {
    pub fn new<V: Into<ScalarValue>>(
        name: String,
        value: V,
        start: usize,
        end: usize,
        expand_left: bool,
        expand_right: bool,
    ) -> Self {
        Mark {
            name,
            value: value.into(),
            start,
            end,
            expand_left,
            expand_right,
        }
    }
}
