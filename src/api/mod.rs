//! This mod is meant to hold most of the code for the library's client-facing API.
mod client;
mod configuration;
mod placeholder_impl;
mod state_machine;

// Anything `pub use` here will be published to lib.
pub use client::RaftClientApi;
pub use client::WriteToLogError;
pub use client::WriteToLogInput;
pub use client::WriteToLogOutput;
pub use configuration::create_raft_client;
pub use configuration::RaftConfig;
pub use state_machine::LocalStateMachineApplier;
pub use state_machine::StateMachineOutput;
