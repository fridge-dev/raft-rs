use std::io;

/// Index is an index of an entry in the log; i.e. a log entry's index.
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq)]
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
}

/// Log is an append only log intended for use as a replicated commit log in a database.
pub trait Log<E: Entry> {
    /// append() appends a log entry to the log at the next log entry index, then returns
    /// the log entry index that was just used to append the entry.
    fn append(&mut self, entry: E) -> Result<Index, io::Error>;

    fn read(&mut self, index: Index) -> Result<Option<E>, io::Error>;

    fn truncate(&mut self, index: Index);

    /// next_index returns the next index that will be used to append an entry.
    fn next_index(&self) -> Index;
}

pub trait Entry: Clone + From<Vec<u8>> + Into<Vec<u8>> {}

// TODO:2 see if unneeded and delete.
#[derive(Debug)]
pub struct RawEntry {
    data_blob: Vec<u8>,
}

impl RawEntry {
    pub fn new(data_blob: Vec<u8>) -> Self {
        RawEntry { data_blob }
    }
}
