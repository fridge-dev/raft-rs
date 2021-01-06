use crate::commitlog::{Entry, Index, Log};
use std::io::Error;
use std::marker::PhantomData;

// Based on https://thehoard.blog/how-kafkas-storage-internals-work-3a29b02e026
pub struct SegmentedDiskLog<E: Entry> {
    // TODO:3 implement a durable commit log.
    _pd: PhantomData<E>,
}

// Generic config for initializing any type of disk-based commit log, independent of data
// model and algorithm.
pub struct StorageConfig {
    pub directory: String,
}

impl<E: Entry> SegmentedDiskLog<E> {
    pub fn new(_: StorageConfig) -> Self {
        SegmentedDiskLog {
            // Nothing
            _pd: Default::default(),
        }
    }
}

#[allow(unused_variables)]
impl<E: Entry> Log<E> for SegmentedDiskLog<E> {
    fn append(&mut self, entry: E) -> Result<Index, Error> {
        unimplemented!()
    }

    fn read(&mut self, index: Index) -> Result<Option<E>, Error> {
        unimplemented!()
    }

    fn truncate(&mut self, index: Index) {
        unimplemented!()
    }

    fn next_index(&self) -> Index {
        unimplemented!()
    }
}
