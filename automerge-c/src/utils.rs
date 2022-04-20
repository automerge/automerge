use crate::AMresult;

impl<'a> From<AMresult<'a>> for *mut AMresult<'a> {
    fn from(b: AMresult<'a>) -> Self {
        Box::into_raw(Box::new(b))
    }
}
