#![allow(dead_code)] // TODO:1.5 remove

mod actor;
mod api;
mod commitlog;
mod replica;
mod grpc {
    include!("../generated/raft.rs");
}

pub use api::create_raft_client;
pub use api::ClusterInfo;
pub use api::LocalStateMachineApplier;
pub use api::MemberInfo;
pub use api::RaftClientConfig;
pub use api::ReplicatedStateMachine;
pub use api::StateMachineOutput;
pub use api::WriteToLogApiError;
pub use api::WriteToLogApiInput;
pub use api::WriteToLogApiOutput;

// Learning 1: `crate::{root_mod}` should not have any code. Just `mod` and `pub use` statements.
// Learning 2: All `mod` statements, anywhere, should not be `pub`. Only export `pub` via individual
//             use statements.
//
// This keeps the `crate::{root_mod}` root_mod only responsible for exporting types to the rest of
// crate, and allows me to organize my root_mod impl however I want. This is because I really like
// go's style of packaging/visibility and I'm going for something similar here.
