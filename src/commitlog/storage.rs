use crate::commitlog::{Entry, Index, Log};
use std::io::Error;

// Based on https://thehoard.blog/how-kafkas-storage-internals-work-3a29b02e026
pub struct SegmentedDiskLog {
    // TODO:3 implement a durable commit log.
}

// Generic config for initializing any type of disk-based commit log, independent of data
// model and algorithm.
pub struct StorageConfig {
    pub directory: String,
}

impl SegmentedDiskLog {
    pub fn new(_: StorageConfig) -> Self {
        SegmentedDiskLog {
            // Nothing
        }
    }
}

#[allow(unused_variables)]
impl Log for SegmentedDiskLog {
    fn append(&mut self, entry: Entry) -> Result<Index, Error> {
        unimplemented!()
    }

    fn read(&mut self, index: Index) -> Result<Option<&Entry>, Error> {
        unimplemented!()
    }

    fn truncate(&mut self, index: Index) {
        unimplemented!()
    }

    fn next_index(&self) -> Index {
        unimplemented!()
    }
}
