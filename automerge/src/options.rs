#[derive(Debug, Default)]
pub struct ApplyOptions<'a, Obs> {
    pub op_observer: Option<&'a mut Obs>,
}

impl<'a, Obs> ApplyOptions<'a, Obs> {
    pub fn with_op_observer(mut self, op_observer: &'a mut Obs) -> Self {
        self.op_observer = Some(op_observer);
        self
    }

    pub fn set_op_observer(&mut self, op_observer: &'a mut Obs) -> &mut Self {
        self.op_observer = Some(op_observer);
        self
    }
}
