mod in_memory;
mod log;
mod storage;

// TODO:2 move commitlog to its own crate/repo.
pub use in_memory::InMemoryLog;
pub use log::Entry;
pub use log::Index;
pub use log::Log;
