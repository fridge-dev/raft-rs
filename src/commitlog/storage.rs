use crate::commitlog::{Log, Entry, Index};
use std::io::Error;

// Based on https://thehoard.blog/how-kafkas-storage-internals-work-3a29b02e026
struct SegmentedDiskLog {
    // TODO:3 implement a durable commit log.
}

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