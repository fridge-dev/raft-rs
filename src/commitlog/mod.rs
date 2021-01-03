mod factory;
mod in_memory;
mod log;
mod storage;

pub use factory::InMemoryLogFactory;
pub use factory::LogConfig;
pub use factory::LogFactory;
pub use factory::SegmentedDiskLogFactory;
pub use in_memory::InMemoryLog;
pub use log::Entry;
pub use log::Index;
pub use log::Log;
