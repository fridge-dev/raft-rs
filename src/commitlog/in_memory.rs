use crate::commitlog::{Entry, Log, Index};
use std::io;

// I am focusing on learning raft's consensus algorithm, not strictly about exactly how to
// persist the data durably. I will theoretically model it here.
//
// To improve on this, do something similar to: https://thehoard.blog/how-kafkas-storage-internals-work-3a29b02e026
pub struct InMemoryLog {
    log: Vec<Entry>
}

impl InMemoryLog {
    pub fn new() -> Self {
        InMemoryLog {
            log: vec![],
        }
    }
}

impl Log for InMemoryLog {
    fn append(&mut self, entry: Entry) -> Result<Index, io::Error> {
        self.log.push(entry);

        Ok(Index((self.log.len() - 1) as u64))
    }

    fn read(&mut self, index: Index) -> Result<Option<&Entry>, io::Error> {
        Ok(self.log.get(index.0 as usize))
    }

    fn truncate(&mut self, index: Index) {
        self.log.truncate(index.0 as usize)
    }

    fn next_index(&self) -> Index {
        Index(self.log.len() as u64)
    }
}