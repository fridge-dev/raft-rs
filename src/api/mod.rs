//! This mod is meant to hold most of the code for the library's client-facing API.
mod client;
mod commit_stream;
mod event_bus;
mod options;
mod replicated_log;
mod types;
mod wiring;

pub use client::RaftClient;
pub use commit_stream::RaftCommitStream;
pub use commit_stream::RaftCommittedEntry;
pub use event_bus::RaftElectionState;
pub use event_bus::RaftEvent;
pub use event_bus::RaftEventListener;
pub use options::RaftOptions;
pub use replicated_log::EnqueueEntryError;
pub use replicated_log::EnqueueEntryInput;
pub use replicated_log::EnqueueEntryOutput;
pub use replicated_log::ReplicatedLog;
pub use types::RaftEntryId;
pub use types::RaftLeaderInfo;
pub use types::RaftMemberInfo;
pub use types::RaftMemberInfoBlob;
pub use wiring::try_create_raft_client;
pub use wiring::RaftClientConfig;

// So Replica can access commit stream.
pub(crate) use commit_stream::RaftCommitStreamPublisher;
