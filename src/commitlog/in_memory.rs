use crate::commitlog::{Entry, Index, Log};
use std::io;
use std::marker::PhantomData;

// I am focusing on learning raft's consensus algorithm, not strictly about exactly how to
// persist the data durably. I will theoretically model it here.
//
// To improve on this, do something similar to: https://thehoard.blog/how-kafkas-storage-internals-work-3a29b02e026
pub struct InMemoryLog<E: Entry> {
    // We don't *need* to convert these to bytes. We could just hold the original entry in memory,
    // but we want to exercise the conversion logic.
    log: Vec<Vec<u8>>,
    _pd: PhantomData<E>,
}

impl<E: Entry> InMemoryLog<E> {
    pub fn new() -> Self {
        InMemoryLog {
            log: vec![],
            _pd: PhantomData::default(),
        }
    }
}

impl<E: Entry> Log<E> for InMemoryLog<E> {
    fn append(&mut self, entry: E) -> Result<Index, io::Error> {
        self.log.push(entry.into());

        Ok(Index::from(self.log.len() - 1))
    }

    fn read(&self, index: Index) -> Result<Option<E>, io::Error> {
        let opt_entry = self.log
            .get(index.val() as usize)
            .cloned()
            .map(|b| E::from(b));

        Ok(opt_entry)
    }

    fn truncate(&mut self, index: Index) {
        self.log.truncate(index.val() as usize)
    }

    fn next_index(&self) -> Index {
        Index::from(self.log.len())
    }
}
