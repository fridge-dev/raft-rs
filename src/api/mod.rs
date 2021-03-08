//! This mod is meant to hold most of the code for the library's client-facing API.
mod client;
mod configuration;
mod factory;

pub use client::CommitStream;
pub use client::CommittedEntry;
pub use client::EntryKey;
pub use client::ReplicatedLog;
pub use client::StartReplicationError;
pub use client::StartReplicationInput;
pub use client::StartReplicationOutput;
pub use configuration::ClusterInfo;
pub use configuration::MemberInfo;
pub use configuration::RaftClientConfig;
pub use factory::create_raft_client;

// So Replica can access commit stream.
pub(crate) use client::CommitStreamPublisher;
