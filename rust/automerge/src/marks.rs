#[derive(Debug, Clone)]
pub struct MarkRange {
    pub start: usize,
    pub end: usize,
    pub expand_left: bool,
    pub expand_right: bool,
}

impl MarkRange {
    pub fn new(start: usize, end: usize, expand_left: bool, expand_right: bool) -> Self {
        MarkRange {
            start,
            end,
            expand_left,
            expand_right,
        }
    }
}
