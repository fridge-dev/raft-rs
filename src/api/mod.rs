//! This mod is meant to hold most of the code for the library's client-facing API.
mod client;
mod commit_stream;
mod event_bus;
mod factory;
mod options;

pub use client::EnqueueEntryError;
pub use client::EnqueueEntryInput;
pub use client::EnqueueEntryOutput;
pub use client::EntryId;
pub use client::LeaderInfo;
pub use client::MemberInfoBlob;
pub use client::ReplicatedLog;
pub use commit_stream::CommitStream;
pub use commit_stream::CommittedEntry;
pub use event_bus::ElectionEvent;
pub use event_bus::Event;
pub use event_bus::EventListener;
pub use factory::create_raft_client;
pub use factory::ClusterInfo;
pub use factory::MemberInfo;
pub use factory::RaftClient;
pub use factory::RaftClientConfig;
pub use options::RaftOptions;

// So Replica can access commit stream.
pub(crate) use commit_stream::CommitStreamPublisher;
