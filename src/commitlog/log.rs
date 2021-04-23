use std::{fmt, io};

#[derive(Copy, Clone, PartialOrd, PartialEq, Ord, Eq)]
struct U64NonZero(u64);

impl U64NonZero {
    fn new(val: u64) -> Self {
        assert_ne!(val, 0);
        U64NonZero(val)
    }
}

/// Index is an index of an entry in the log; i.e. a log entry's index.
#[derive(Copy, Clone, PartialOrd, PartialEq, Ord, Eq)]
pub struct Index(U64NonZero);

impl Index {
    pub fn new(index: u64) -> Self {
        Index(U64NonZero::new(index))
    }

    pub fn new_usize(index: usize) -> Self {
        Self::new(index as u64)
    }

    pub fn start_index() -> Self {
        Self::new(1)
    }

    pub fn as_u64(&self) -> u64 {
        self.0 .0
    }

    pub fn plus(&self, delta: u64) -> Index {
        Index::new(self.as_u64() + delta)
    }

    pub fn minus(&self, delta: u64) -> Index {
        Index::new(self.as_u64() - delta)
    }

    pub fn checked_minus(&self, delta: u64) -> Option<Index> {
        let new_value = self.as_u64() - delta;
        if new_value > 0 {
            Some(Index::new(new_value))
        } else {
            None
        }
    }
}

impl fmt::Debug for Index {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0 .0)
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

// Choice of Vec<u8> vs Bytes will depend on whats easier for disk to use.
pub trait Entry: Clone + From<Vec<u8>> + Into<Vec<u8>> {}
