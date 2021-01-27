#![allow(dead_code)] // TODO:1.5 remove

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
pub use api::RaftClientApi;
pub use api::RaftClientConfig;
pub use api::StateMachineOutput;
pub use api::WriteToLogError;
pub use api::WriteToLogInput;
pub use api::WriteToLogOutput;

// Learning 1: `create::{root_mod}` should not have any code. Just `mod` and `pub use` statements.
// Learning 2: All `mod` statements, anywhere, should not be `pub`. Only export `pub` via individual
//             use statements.
//
// This keeps the `crate::{root_mod}` root_mod only responsible for exporting types to the rest of
// crate, and allows me to organize my root_mod impl however I want.
