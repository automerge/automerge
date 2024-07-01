use automerge as am;

use std::any::type_name;

use smol_str::SmolStr;

use crate::byte_span::AMbyteSpan;

/// \struct AMindex
/// \installed_headerfile
/// \brief An item index.
#[derive(PartialEq)]
pub enum AMindex {
    /// A UTF-8 string key variant.
    Key(SmolStr),
    /// A 64-bit unsigned integer position variant.
    Pos(usize),
}

impl TryFrom<&AMindex> for AMbyteSpan {
    type Error = am::AutomergeError;

    fn try_from(item: &AMindex) -> Result<Self, Self::Error> {
        use am::AutomergeError::InvalidValueType;
        use AMindex::*;

        if let Key(key) = item {
            return Ok(key.into());
        }
        Err(InvalidValueType {
            expected: type_name::<SmolStr>().to_string(),
            unexpected: type_name::<usize>().to_string(),
        })
    }
}

impl TryFrom<&AMindex> for usize {
    type Error = am::AutomergeError;

    fn try_from(item: &AMindex) -> Result<Self, Self::Error> {
        use am::AutomergeError::InvalidValueType;
        use AMindex::*;

        if let Pos(pos) = item {
            return Ok(*pos);
        }
        Err(InvalidValueType {
            expected: type_name::<usize>().to_string(),
            unexpected: type_name::<SmolStr>().to_string(),
        })
    }
}

/// \ingroup enumerations
/// \enum AMidxType
/// \installed_headerfile
/// \brief The type of an item's index.
#[derive(Eq, PartialEq)]
#[repr(C)]
pub enum AMidxType {
    /// The default tag, not a type signifier.
    Default = 0,
    /// A UTF-8 string view key.
    Key,
    /// A 64-bit unsigned integer position.
    Pos,
}

impl Default for AMidxType {
    fn default() -> Self {
        Self::Default
    }
}

impl From<&AMindex> for AMidxType {
    fn from(index: &AMindex) -> Self {
        use AMindex::*;

        match index {
            Key(_) => Self::Key,
            Pos(_) => Self::Pos,
        }
    }
}
