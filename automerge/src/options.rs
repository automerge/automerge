use crate::op_observer::OpObserver;

#[derive(Debug, Default)]
pub struct ApplyOptions<'a> {
    pub op_observer: Option<&'a mut OpObserver>,
}

impl<'a> ApplyOptions<'a> {
    pub fn with_op_observer(mut self, op_observer: &'a mut OpObserver) -> Self {
        self.op_observer = Some(op_observer);
        self
    }

    pub fn set_op_observer(&mut self, op_observer: &'a mut OpObserver) -> &mut Self {
        self.op_observer = Some(op_observer);
        self
    }
}

impl<'a> From<Option<&'a mut OpObserver>> for ApplyOptions<'a> {
  fn from(o: Option<&'a mut OpObserver>) -> Self {
    ApplyOptions { op_observer: o }
  }
}
