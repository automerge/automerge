use crate::types::Clock;
use crate::ChangeHash;
use std::borrow::Cow;
use std::fmt;

#[derive(PartialOrd, Ord, Debug, Clone, Eq, Hash, PartialEq)]
pub enum Branch {
    Main,
    Name(String),
}

pub(crate) fn unpack_message<'a, S: From<&'a str>>(packed: &'a str) -> (Option<S>, Branch) {
    let mut parts = packed.split('🧇');
    let message = parts
        .next()
        .and_then(|s| if s.is_empty() { None } else { Some(s.into()) });
    let branch = parts.next().map(Branch::new).unwrap_or_default();
    (message, branch)
}

//pub(crate) fn pack_message(_message: Option<&String>, _branch: &Branch) -> Option<Cow<'static, smol_str::SmolStr>> {
pub(crate) fn pack_message(message: Option<&String>, branch: &Branch) -> Option<smol_str::SmolStr> {
    match (message, branch) {
        (None, Branch::Main) => None,
        (Some(m), Branch::Main) => Some(m.into()),
        (Some(m), b) => Some(format!("{}🧇{}", m, b).into()),
        (None, b) => Some(format!("🧇{}", b).into()),
    }
}

impl Branch {
    pub fn new(name: &str) -> Self {
        if name == Self::main_name() {
            Self::Main
        } else {
            Self::Name(name.to_owned())
        }
    }

    pub fn main_name() -> &'static str {
        "main"
    }

    /*
        pub fn to_string(&self) -> String {
            match self {
                Self::Main => Self::main_name().to_owned(),
                Self::Name(n) => n.to_owned(),
            }
        }
    */
}

impl Default for Branch {
    fn default() -> Self {
        Branch::Main
    }
}

impl<'a> Default for OpRef2<'a> {
    fn default() -> Self {
        Self::Default
    }
}

impl fmt::Display for Branch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Main => write!(f, "{}", Self::main_name()),
            Self::Name(n) => write!(f, "{}", n),
        }
    }
}

#[derive(Debug)]
pub enum OpRef {
    Branch(Branch),
    Heads(Vec<ChangeHash>),
}

#[derive(Debug)]
pub enum OpRef2<'a> {
    Default,
    Branch(Cow<'a, Branch>),
    Heads(Cow<'a, [ChangeHash]>),
}

impl<'a> From<&'a [ChangeHash]> for OpRef2<'a> {
    fn from(heads: &'a [ChangeHash]) -> Self {
        Self::Heads(Cow::Borrowed(heads))
    }
}

impl From<Vec<ChangeHash>> for OpRef2<'static> {
    fn from(heads: Vec<ChangeHash>) -> Self {
        Self::Heads(Cow::Owned(heads))
    }
}

impl From<Option<Vec<ChangeHash>>> for OpRef2<'static> {
    fn from(heads: Option<Vec<ChangeHash>>) -> Self {
        match heads {
            Some(heads) => Self::Heads(Cow::Owned(heads)),
            None => Self::Default,
        }
    }
}

impl<'a> From<Option<&'a Vec<ChangeHash>>> for OpRef2<'a> {
    fn from(heads: Option<&'a Vec<ChangeHash>>) -> Self {
        match heads {
            Some(heads) => Self::Heads(Cow::Borrowed(heads)),
            None => Self::Default,
        }
    }
}

impl From<Branch> for OpRef2<'static> {
    fn from(branch: Branch) -> Self {
        Self::Branch(Cow::Owned(branch))
    }
}

impl<'a> From<&'a Branch> for OpRef2<'a> {
    fn from(branch: &'a Branch) -> Self {
        Self::Branch(Cow::Borrowed(branch))
    }
}

impl<'a> From<OpRef2<'a>> for Option<OpRef> {
    fn from(o: OpRef2<'a>) -> Self {
        match o {
            OpRef2::Default => None,
            OpRef2::Branch(b) => Some(OpRef::Branch(b.into_owned())),
            OpRef2::Heads(h) => Some(OpRef::Heads(h.to_vec())),
        }
    }
}

impl<'a> From<&'a [ChangeHash]> for OpRef {
    fn from(a: &'a [ChangeHash]) -> Self {
        OpRef::Heads(a.to_vec())
    }
}

impl From<Vec<ChangeHash>> for OpRef {
    fn from(a: Vec<ChangeHash>) -> Self {
        OpRef::Heads(a)
    }
}

pub(crate) trait BranchScope {
    fn scope_branch(&self, branch: &Option<OpRef>) -> Option<Clock>;
}
