#![allow(dead_code)] // TODO:3 remove

use crate::commitlog::{Entry, Index, Log};
use std::io;
use std::io::Error;
use std::marker::PhantomData;

// TODO:3 Implement a durable commit log based on algorithm described in
// https://thehoard.blog/how-kafkas-storage-internals-work-3a29b02e026.
//
// https://github.com/travisjeffery/jocko/tree/master/commitlog is a good (golang) reference.
pub struct SegmentedDiskLog<E: Entry> {
    _pd: PhantomData<E>,
}

pub struct SegmentedDiskLogConfig {
    pub directory: String,
    pub max_data_log_size_bytes: u64,
}

impl<E: Entry> SegmentedDiskLog<E> {
    pub fn create(_: SegmentedDiskLogConfig) -> Result<Self, io::Error> {
        Ok(SegmentedDiskLog {
            // Nothing
            _pd: Default::default(),
        })
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
