use crate::commitlog::{Log, InMemoryLog};
use std::io;
use crate::commitlog::storage::{SegmentedDiskLog, StorageConfig};

// -- Log factory --
pub trait LogFactory<L: Log> {
    fn try_create_log(&self, config: LogConfig) -> Result<L, io::Error>;
}

pub struct LogConfig {
    pub cluster_id: String,
}

// -- InMemoryLogFactory --

pub struct InMemoryLogFactory;

impl InMemoryLogFactory {
    pub fn new() -> Self {
        InMemoryLogFactory
    }
}

impl LogFactory<InMemoryLog> for InMemoryLogFactory {
    fn try_create_log(&self, _: LogConfig) -> Result<InMemoryLog, io::Error> {
        Ok(InMemoryLog::new())
    }
}

// -- SegmentedDiskLogFactory --

pub struct SegmentedDiskLogFactory {

}

impl LogFactory<SegmentedDiskLog> for SegmentedDiskLogFactory {
    fn try_create_log(&self, config: LogConfig) -> Result<SegmentedDiskLog, io::Error> {
        let sdl = SegmentedDiskLog::new(StorageConfig {
            directory: format!("/raft/commitlog/cluster_{}/", config.cluster_id),
        });

        Ok(sdl)
    }
}
