/// Optional metadata for a commit.
#[derive(Debug, Default)]
pub struct CommitOptions {
    /// A message which describes the commit
    pub message: Option<String>,
    /// The unix timestamp (in seconds) of the commit (purely advisory, not used in conflict resolution)
    pub time: Option<i64>,
}

impl CommitOptions {
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
}
