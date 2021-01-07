use crate::commitlog::{Entry, Index, Log};
use std::io::Error;
use std::marker::PhantomData;

// Based on https://thehoard.blog/how-kafkas-storage-internals-work-3a29b02e026
//
// Bytes:
// 0    : crc
// 1    : crc
// 2    : crc
// 3    : crc
// 4    : version
// 5    : attributes
// 6    : timestamp
// 7    : timestamp
// 8    : timestamp
// 9    : timestamp
// 10   : timestamp
// 11   : timestamp
// 12   : timestamp
// 13   : timestamp
// 14   : data size
// 15   : data size
// 16   : data size
// 17   : data size
// V0   : data
// V1   : data
// V2   : data
// ...
// Vn-2 : data
// Vn-1 : data
// Vn   : data
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

    fn read(&self, index: Index) -> Result<Option<E>, Error> {
        unimplemented!()
    }

    fn truncate(&mut self, index: Index) {
        unimplemented!()
    }

    fn next_index(&self) -> Index {
        unimplemented!()
    }
}
