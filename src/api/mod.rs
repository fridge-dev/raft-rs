//! This mod is meant to hold most of the code for the library's client-facing API.
mod client;
mod configuration;
mod factory;
mod state_machine;

pub use client::ReplicatedStateMachine;
pub use client::WriteToLogError;
pub use client::WriteToLogInput;
pub use client::WriteToLogOutput;
pub use configuration::ClusterInfo;
pub use configuration::MemberInfo;
pub use configuration::RaftClientConfig;
pub use factory::create_raft_client;
pub use state_machine::LocalStateMachineApplier;
pub use state_machine::StateMachineOutput;
