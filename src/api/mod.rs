//! This mod is meant to hold most of the code for the library's client-facing API.
mod client;
mod configuration;
mod factory;
mod state_machine;

pub use client::ReplicatedStateMachine;
pub use client::WriteToLogApiError;
pub use client::WriteToLogApiInput;
pub use client::WriteToLogApiOutput;
pub use configuration::ClusterInfo;
pub use configuration::MemberInfo;
pub use configuration::RaftClientConfig;
pub use factory::create_raft_client;
pub use state_machine::LocalStateMachineApplier;
pub use state_machine::StateMachineOutput;
