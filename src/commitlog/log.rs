use std::io;

/// Index is an index of an entry in the log; i.e. a log entry's index.
#[derive(Debug, Copy, Clone)]
pub struct Index(pub u64);

/// Log is an append only log intended for use as a commit log in a database.
pub trait Log {
    /// append() appends a log entry to the log at the next log entry index, then returns
    /// the log entry index that was just used to append the entry.
    fn append(&mut self, entry: Entry) -> Result<Index, io::Error>;

    fn read(&mut self, index: Index) -> Result<Option<&Entry>, io::Error>;

    fn truncate(&mut self, index: Index);

    /// next_index returns the next index that will be used to append an entry.
    fn next_index(&self) -> Index;
}

#[derive(Debug)]
pub struct Entry {
    data_blob: Vec<u8>,
}

impl Entry {
    pub fn new(data_blob: Vec<u8>) -> Self {
        Entry {
            data_blob,
        }
    }
}
