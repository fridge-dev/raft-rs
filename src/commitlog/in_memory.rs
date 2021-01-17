use crate::commitlog::{Entry, Index, Log};
use std::io;
use std::marker::PhantomData;

// I am focusing on learning raft's consensus algorithm, not strictly about exactly how to
// persist the data durably. I will theoretically model it here.
//
// To improve on this, see storage.rs.
pub struct InMemoryLog<E: Entry> {
    // We don't *need* to convert these to bytes. We could just hold the original entry in memory,
    // but we want to exercise the conversion logic.
    log: Vec<Vec<u8>>,
    _pd: PhantomData<E>,
}

impl<E: Entry> InMemoryLog<E> {
    pub fn create() -> Result<Self, io::Error> {
        Ok(InMemoryLog {
            log: vec![],
            _pd: PhantomData::default(),
        })
    }

    fn vec_index(index: Index) -> usize {
        // Log API states that Index starts from 1.
        (index.val() - 1) as usize
    }
}

impl<E: Entry> Log<E> for InMemoryLog<E> {
    fn append(&mut self, entry: E) -> Result<Index, io::Error> {
        self.log.push(entry.into());

        Ok(Index::from(self.log.len()))
    }

    fn read(&self, index: Index) -> Result<Option<E>, io::Error> {
        let vec_index = Self::vec_index(index);
        let opt_entry = self.log.get(vec_index).cloned().map(|b| E::from(b));

        Ok(opt_entry)
    }

    fn truncate(&mut self, index: Index) {
        let vec_index = Self::vec_index(index);
        self.log.truncate(vec_index)
    }

    fn next_index(&self) -> Index {
        Index::from(self.log.len() + 1)
    }
}
