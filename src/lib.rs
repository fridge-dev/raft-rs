#![feature(ready_macro)]

mod actor;
mod api;
mod commitlog;
mod replica;
mod grpc {
    include!("../generated/raft.rs");
}
mod server;

pub use api::create_raft_client;
pub use api::ClusterInfo;
pub use api::CommitStream;
pub use api::CommittedEntry;
pub use api::ElectionEvent;
pub use api::EnqueueEntryError;
pub use api::EnqueueEntryInput;
pub use api::EnqueueEntryOutput;
pub use api::EntryId;
pub use api::Event;
pub use api::EventListener;
pub use api::LeaderInfo;
pub use api::MemberInfo;
pub use api::MemberInfoBlob;
pub use api::RaftClient;
pub use api::RaftClientConfig;
pub use api::RaftOptions;
pub use api::ReplicatedLog;

// Learning 1: `crate::{root_mod}` should not have any code. Just `mod` and `pub use` statements.
// Learning 2: All `mod` statements, anywhere, should not be `pub`. Only export `pub` via individual
//             use statements.
//
// This keeps the `crate::{root_mod}` root_mod only responsible for exporting types to the rest of
// crate, and allows me to organize my root_mod impl however I want. This is because I really like
// go's style of packaging/visibility and I'm going for something similar here.
