use std::io;

/// Index is an index of an entry in the log; i.e. a log entry's index.
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq)]
pub struct Index(u64);

impl Index {
    pub fn new(index: u64) -> Self {
        Index(index)
    }

    pub fn from(index: usize) -> Self {
        Index(index as u64)
    }

    pub fn val(&self) -> u64 {
        self.0
    }

    pub fn incr(&mut self, delta: u64) {
        self.0 += delta;
    }

    pub fn plus(&self, delta: u64) -> Index {
        Index(self.0 + delta)
    }

    pub fn decr(&mut self, delta: u64) {
        self.0 -= delta;
    }

    pub fn minus(&self, delta: u64) -> Index {
        Index(self.0 - delta)
    }
}

/// Log is an append only log intended for use as a replicated commit log in a database.
///
/// Log indexes entries starting from 1. There will be no entry existing at index 0. The first
/// entry is written at index 1.
pub trait Log<E: Entry> {
    /// append() appends a log entry to the log at the next log entry index, then returns
    /// the log entry index that was just used to append the entry.
    fn append(&mut self, entry: E) -> Result<Index, io::Error>;

    /// Read log entry at specified index.
    fn read(&self, index: Index) -> Result<Option<E>, io::Error>;

    /// Soft-deletes anything starting at `index` and later. Soft-deletion makes this infallible.
    /// If hard-deletion is required, add a new method.
    fn truncate(&mut self, index: Index);

    /// next_index returns the next index that will be used to append an entry.
    fn next_index(&self) -> Index;
}

pub trait Entry: Clone + From<Vec<u8>> + Into<Vec<u8>> {}
