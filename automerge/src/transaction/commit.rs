/// Optional metadata for a commit.
#[derive(Debug, Default, Clone)]
pub struct CommitOptions {
    pub(crate) message: Option<String>,
    pub(crate) time: Option<i64>,
}

impl CommitOptions {
    /// Add a message to the commit.
    pub fn with_message(mut self, message: String) -> Self {
        self.message = Some(message);
        self
    }

    /// Add a message to the commit.
    pub fn set_message(&mut self, message: String) -> &mut Self {
        self.message = Some(message);
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
}
