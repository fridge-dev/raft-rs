//! == Raft Replica Architecture ==
//!
//! **Top level library API called by application**
//!
//! ```no
//! ReplicatedStateMachine {
//!     // This will include router to either call local leader or redirect to remote leader.
//!     execute(command: byte[]) -> byte[]
//!
//!     // What if application layer wants to rely on raft leader election?
//!     // They shouldn't need it. They should only need a strictly serializable database. They can
//!     // accomplish leader election via more simple/primitive operations like locking.
//!     // This means they will almost always build a simpler abstraction on top of this interface.
//!     // They should also perform reads via local state machine applier's state. It is guaranteed
//!     // to have committed data. Not guaranteed to be fresh. Still need to figure out consistent
//!     // reads, but we already have consistent read-update-write.
//!
//!     // TODO:2 figure out read API
//! }
//! ```
//!
//! **Dependencies provided application**
//!
//! * Disk(?) No. We can create and use our own APIs for interacting with disk.
//! ** For commit log, we can use our own logic. Maybe we separate crate into fridge-dev/commit-log.
//! ** For stable storage, same as above. Separated crate not needed.
//! * State Machine Applier - app implements library interface.
//! * Basic cluster configuration: who are members, who am I? etc.
//!
//! Code:
//!
//! ```no
//! LocalStateMachineApplier {
//!     apply(command: byte[]) -> byte[]
//! }
//!
//! ReplicatedStateMachine rsm = new ReplicatedStateMachineImpl(
//!     localApplier: LocalStateMachineApplier,
//!     directory: "/raft/",
//!     clusterConfig: {...},
//! )
//! ```
//!
//! **Internal class structure**
//!
//! `ReplicatedStateMachineImpl`
//! - Depends on `Replica`.
//! - Basically the frontend for application use.
//! - Tracks leadership via updates from local Replica and knows to route requests to local Replica
//!   or remote raft frontend.
//! - Wait. Does this means we need something like `ReplicatedStateMachineRpcServer`? I think so.
//! - Is this extra abstraction needed? Maybe it should be a separate client library that is cluster
//!   aware and has many `ReplicatedStateMachineRpcClient` instances. I guess it depends on how an
//!   application needs to use raft. Do we want to run in same process? Probably easier not to. But
//!   how would client provide state machine applier if not in process? I think I need better
//!   definition of use case. What I'm imagining (endpoint = ip/port):
//!     1. Generic raft library that uses RPC/HTTP on {raft_endpoint} to coordinate with peers. Simply
//!        replicates blobs.
//!     2. There is an application specific backend RPC server on {app_be_endpoint}. It validates/accepts
//!        transitions with app-specific logic to provide a strictly serializable key-value store.
//!        It calls into raft lib. It does not follow redirects.
//!     3. Application is a web service over {app_fe_endpoint} that exposes the API to external service
//!        clients. It does basic frontend stuff (authN, authZ, throttling, etc) and then forwards
//!        request to the correct {app_be_endpoint}. Internally, it will follow redirects from the
//!        {app_be_endpoint}. It is not fully cluster aware, it just needs to remember the most recent
//!        leader and keep using that until the backend redirects. It can discover the initial leader
//!        at application startup by trying to call the localhost {app_be_endpoint}. Since this layer
//!        follows redirects, a client can make request to any host's {app_fe_endpoint} and the
//!        Application will take care of routing to the correct {app_be_endpoint} leader. This way,
//!        the cluster could actually just sit behind a load balancer and clients call the load balancer
//!        or clients call any host in the cluster and it just works.
//!     4. Some client needs KV store, so they call Application on {app_fe_endpoint} from 3. Their
//!        connection will never need to be redirected.
//!
//! `RaftRpcServer`
//! - Depends on `Replica`.
//! - Basically the frontend for raft algorithm (RequestVote/AppendEntries).
//! - Just listens on raft port and delegates to Replica.
//!
//! `Replica`
//! - Depends on `CommitLog`, `RemotePeers`, ...
//! - Basically the backend.
//! - Implements the single node logic of the raft algorithm.
//!
//! `CommitLog`
//! - Storage layer used by `Replica` for tracking commit log.
//!
//! `RemotePeers`
//! - Depends on `RaftRpcClient`
//! - Abstraction over raw RPC client.
//! - Manages connections to all relevant peers.
//!
//! `RaftRpcClient`
//! - Client for calling remote `RaftRpcServer`.
//!
//!
//! --------------------
//!
//! Copying notes from old gRPC server module:
//!
//! 3 different gRPC servers:
//!
//! Raft data plane ({raft_endpoint}) - this is the primitive building block.
//! - AppendEntries(...)
//! - RequestVote(...)
//!
//! Raft control plane ({raft_endpoint}) - used during raft deployment/cycling (infrequent). They ultimately delegate towards raft data plane.
//! - ObserveCluster(...) - for adding self to cluster
//! - LeaveCluster(...) - for removing self from cluster
//! - UpdateCluster(...) - for existing members to become aware of added/removed members
//!
//! Public facing KV store ({app_endpoint}) - supports redirects. Ultimately results in raft data plane calls.
//! - Get(key)
//! - Put(key, value, options...)
//! - Delete(key, options...)
//!
mod commit_log;
mod election;
mod local_state;
mod peer_client;
mod peers;
mod replica;
mod replica_api;
mod timers;

pub use commit_log::RaftLogEntry;
pub use local_state::PersistentLocalState;
pub use local_state::VolatileLocalState;
pub use peers::ClusterTracker;
pub use peers::InvalidCluster;
pub use peers::ReplicaMetadata;
pub use replica::Replica;
pub use replica::ReplicaConfig;
pub use replica_api::*;

pub(crate) use local_state::Term;
pub(crate) use peers::ReplicaBlob;
pub(crate) use peers::ReplicaId;
