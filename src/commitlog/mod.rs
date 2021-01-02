mod api;
mod in_memory;

#[allow(unused_variables)]
mod storage;

pub use api::Index;
pub use api::Log;
pub use api::Entry;
pub use in_memory::InMemoryLog;
