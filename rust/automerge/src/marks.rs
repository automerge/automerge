#[derive(Debug, Clone)]
pub enum RangeExpand {
    Neither,
    ExpandLeft,
    ExpandRight,
    ExpandBoth,
}

impl RangeExpand {
    pub fn new(left: bool, right: bool) -> Self {
        match (left, right) {
            (true, true) => Self::ExpandBoth,
            (true, false) => Self::ExpandLeft,
            (false, true) => Self::ExpandRight,
            (false, false) => Self::Neither,
        }
    }

    pub fn expand_left(&self) -> bool {
        match self {
            Self::ExpandLeft => true,
            Self::ExpandBoth => true,
            _ => false,
        }
    }

    pub fn expand_right(&self) -> bool {
        match self {
            Self::ExpandRight => true,
            Self::ExpandBoth => true,
            _ => false,
        }
    }
}
