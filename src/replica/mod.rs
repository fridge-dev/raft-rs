//! == Raft Replica Architecture ==
//!
//! **Top level library API called by application**
//!
//! ```no
//! ReplicatedLog {
//!     // This will include router to either call local leader or redirect to remote leader.
//!     append(data: byte[]) -> EntryId
//!
//!     // Block until we receive a committed entry. It is up to caller to apply committed
//!     // entries to their state machine.
//!     wait_for_next_commit() -> (data: byte[], id: EntryId)
//! }
//! ```
//!
//! **Dependencies provided application**
//!
//! * Disk/log file implementation. We will provide default impls by depending on a different crate,
//!   probably `fridge-dev/commit-log`. Consumer can provide their own.
//! ** For stable storage, we will do similar to above.
//! * Basic cluster configuration: who are members, who am I? etc.
//!
//! **Internal class structure**
//!
//! `ReplicatedLogImpl`
//! * Depends on `Replica`.
//! * Tracks leadership and rejects append calls if local Replica is not leader. It will provide
//!   redirect info to the caller.
//!
//! `RaftRpcServer`
//! * Depends on `Replica`.
//! * Serves RPCs for raft algorithm (RequestVote/AppendEntries).
//! * Just listens on raft port and delegates to Replica.
//!
//! `Replica`
//! * Depends on `CommitLog`, `RemotePeers`, ...
//! * Core Raft logic, Implements the single node logic of the raft algorithm.
//!
//! `CommitLog`
//! * Storage layer used by `Replica` for tracking commit log.
//!
//! `RemotePeers`
//! * Depends on `RaftRpcClient`
//! * Abstraction over raw RPC client.
//! * Manages connections to all relevant peers.
//!
//! `RaftRpcClient`
//! * Client for calling remote `RaftRpcServer`.
//!
//! --------------------
//!
//! Copying notes from old gRPC server module:
//!
//! 3 different gRPC servers:
//!
//! Raft data plane ({raft_endpoint}) - this is the primitive building block.
//! * AppendEntries(...)
//! * RequestVote(...)
//!
//! Raft control plane ({raft_endpoint}) - used during raft deployment/cycling (infrequent). They ultimately delegate
//! towards raft data plane.
//! * ObserveCluster(...) - for adding self to cluster
//! * LeaveCluster(...) - for removing self from cluster
//! * UpdateCluster(...) - for existing members to become aware of added/removed members
//!
//! Public facing KV store ({app_endpoint}) - supports redirects. Ultimately results in raft data plane calls.
//! * Get(key)
//! * Put(key, value, options...)
//! * Delete(key, options...)
mod election;
mod local_state;
mod peer_client;
mod peers;
mod replica;
mod replica_api;
mod replica_wiring;
mod write_ahead_log;

pub(crate) use election::ElectionStateChangeListener;
pub(crate) use election::ElectionStateSnapshot;
pub(crate) use local_state::Term;
pub(crate) use peers::ReplicaId;
pub(crate) use peers::ReplicaInfoBlob;
pub(crate) use peers::ReplicaMetadata;
pub(crate) use replica::Replica;
pub(crate) use replica_api::*;
pub(crate) use replica_wiring::create_replica;
pub(crate) use write_ahead_log::WriteAheadLogEntry;
pub(crate) use write_ahead_log::CommittedEntry;
pub(crate) use write_ahead_log::CommitStream;

// TODO:1 refactor replica wiring and then remove these re-exports
pub(crate) use peers::ClusterTracker;
pub(crate) use local_state::VolatileLocalState;
