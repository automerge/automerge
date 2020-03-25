#[allow(clippy::trivially_copy_pass_by_ref)]
pub(crate) fn is_false(val: &bool) -> bool {
    !val
}

pub(crate) fn make_true() -> bool {
    true
}

pub(crate) fn make_false() -> bool {
    false
}
