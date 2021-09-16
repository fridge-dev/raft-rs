mod in_memory;
mod log;
mod storage;

// TODO:2 move commitlog to its own crate/repo.
pub(crate) use in_memory::InMemoryLog;
pub(crate) use log::Entry;
pub(crate) use log::Index;
pub(crate) use log::Log;
