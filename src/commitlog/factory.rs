use crate::commitlog::storage::{SegmentedDiskLog, StorageConfig};
use crate::commitlog::{Entry, InMemoryLog, Log};
use std::io;

// Note: This is unneeded as I've removed multi-replica-per-host functionality.
// Oh well, leaving it for now.

// -- Log factory --
pub trait LogFactory<E, L>
where
    E: Entry,
    L: Log<E>,
{
    fn try_create_log(&self, config: LogConfig) -> Result<L, io::Error>;
}

pub struct LogConfig {
    pub base_directory: String,
}

// -- InMemoryLogFactory --

pub struct InMemoryLogFactory;

impl InMemoryLogFactory {
    pub fn new() -> Self {
        InMemoryLogFactory
    }
}

impl<E: Entry> LogFactory<E, InMemoryLog<E>> for InMemoryLogFactory {
    fn try_create_log(&self, _: LogConfig) -> Result<InMemoryLog<E>, io::Error> {
        Ok(InMemoryLog::new())
    }
}

// -- SegmentedDiskLogFactory --

pub struct SegmentedDiskLogFactory;

impl<E: Entry> LogFactory<E, SegmentedDiskLog<E>> for SegmentedDiskLogFactory {
    fn try_create_log(&self, config: LogConfig) -> Result<SegmentedDiskLog<E>, io::Error> {
        let sdl = SegmentedDiskLog::new(StorageConfig {
            directory: format!("{}/replicated-commit-log/", config.base_directory),
        });

        Ok(sdl)
    }
}
