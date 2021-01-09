//! This mod is meant to hold most of the code for the library's client-facing API.
mod client;
mod factory;
mod configuration;
mod placeholder_impl;
mod state_machine;

pub use client::RaftClientApi;
pub use client::WriteToLogError;
pub use client::WriteToLogInput;
pub use client::WriteToLogOutput;
pub use factory::create_raft_client;
pub use configuration::RaftClientConfig;
pub use configuration::MemberInfo;
pub use configuration::ClusterInfo;
pub use state_machine::LocalStateMachineApplier;
pub use state_machine::StateMachineOutput;
