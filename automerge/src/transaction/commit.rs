/// Optional metadata for a commit.
#[derive(Debug, Default)]
pub struct CommitOptions<'a, Obs> {
    pub message: Option<String>,
    pub time: Option<i64>,
    pub op_observer: Option<&'a mut Obs>,
}

impl<'a, Obs> CommitOptions<'a, Obs> {
    /// Add a message to the commit.
    pub fn with_message<S: Into<String>>(mut self, message: S) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Add a message to the commit.
    pub fn set_message<S: Into<String>>(&mut self, message: S) -> &mut Self {
        self.message = Some(message.into());
        self
    }

    /// Add a timestamp to the commit.
    pub fn with_time(mut self, time: i64) -> Self {
        self.time = Some(time);
        self
    }

    /// Add a timestamp to the commit.
    pub fn set_time(&mut self, time: i64) -> &mut Self {
        self.time = Some(time);
        self
    }

    pub fn with_op_observer(mut self, op_observer: &'a mut Obs) -> Self {
        self.op_observer = Some(op_observer);
        self
    }

    pub fn set_op_observer(&mut self, op_observer: &'a mut Obs) -> &mut Self {
        self.op_observer = Some(op_observer);
        self
    }
}
