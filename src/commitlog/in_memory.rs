use crate::commitlog::{Entry, Index, Log};
use std::io;

// I am focusing on learning raft's consensus algorithm, not strictly about exactly how to
// persist the data durably. I will theoretically model it here.
//
// To improve on this, do something similar to: https://thehoard.blog/how-kafkas-storage-internals-work-3a29b02e026
pub struct InMemoryLog<E: Entry> {
    log: Vec<E>,
}

impl<E: Entry> InMemoryLog<E> {
    pub fn new() -> Self {
        InMemoryLog { log: vec![] }
    }
}

impl<E: Entry> Log<E> for InMemoryLog<E> {
    fn append(&mut self, entry: E) -> Result<Index, io::Error> {
        self.log.push(entry);

        Ok(Index::from(self.log.len() - 1))
    }

    fn read(&self, index: Index) -> Result<Option<E>, io::Error> {
        Ok(self.log.get(index.val() as usize).cloned())
    }

    fn truncate(&mut self, index: Index) {
        self.log.truncate(index.val() as usize)
    }

    fn next_index(&self) -> Index {
        Index::from(self.log.len())
    }
}
