//! This mod is meant to hold most of the code for the library's client-facing API.
mod client;
mod commit_stream;
mod factory;
mod options;

pub use client::EntryKey;
pub use client::MemberInfoBlob;
pub use client::ReplicatedLog;
pub use client::StartReplicationError;
pub use client::StartReplicationInput;
pub use client::StartReplicationOutput;
pub use commit_stream::CommitStream;
pub use commit_stream::CommittedEntry;
pub use factory::create_raft_client;
pub use factory::ClusterInfo;
pub use factory::MemberInfo;
pub use factory::RaftClientConfig;
pub use options::RaftOptions;

// So Replica can access commit stream.
pub(crate) use commit_stream::CommitStreamPublisher;
