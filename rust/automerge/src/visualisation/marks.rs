use std::collections::HashMap;

use crate::{OpType, types::{OpId, Op}, ScalarValue};

pub(crate) fn print_marks_to_terminal<'a, I: Iterator<Item=&'a Op>>(ops: I) {
    let mut vis = TextVis::new();
    for op in ops {
        vis.append(op);
    }
    vis.print_to_terminal();
}

struct TextVis {
    marks: HashMap<OpId, String>,
    elements: Vec<Element>
}

enum Element {
    BeginMark(bool, String),
    EndMark(bool, String),
    Character(String, bool),
}

impl Element {
    fn as_terminal(&self) -> String {
        match self {
            Self::BeginMark(expand, name) => {
                if *expand {
                    format!("<{},expand>", name)
                } else {
                    format!("<{}, noexpand>", name)
                }
            },
            Self::EndMark(expand, name) => {
                if *expand {
                    format!("</{},expand>", name)
                } else {
                    format!("</{}, noexpand>", name)
                }
            },
            Self::Character(value, visible) => {
                if *visible {
                    format!("\x1b[93m{}\x1b[0m", value)
                } else {
                    value.to_string()
                }
            }
        }
    }
}

impl TextVis {
    fn new() -> Self {
        Self {
            marks: HashMap::new(),
            elements: Vec::new(),
        }
    }

    fn append<'a>(&mut self, op: &'a Op) {
        if !op.insert {
            return;
        }
        match &op.action {
            OpType::Put(ScalarValue::Str(s)) => {
                self.elements.push(Element::Character(s.to_string(), op.visible()));
            },
            OpType::MarkBegin(expand, data) => {
                let mark_name = data.name.to_string();
                self.marks.insert(op.id, mark_name.clone());
                self.elements.push(Element::BeginMark(*expand, mark_name));
            },
            OpType::MarkEnd(expand) => {
                let mark_name = self.marks.get(&op.id.prev()).unwrap().to_string();
                self.elements.push(Element::EndMark(*expand, mark_name));
            },
            _ => {},
        }
    }

    fn print_to_terminal(&self) {
        let output = self.elements.iter().map(|e| e.as_terminal()).collect::<String>();
        println!("{}", output)
    }
}
