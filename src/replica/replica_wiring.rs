use crate::replica::{ClusterTracker, Replica, ElectionStateChangeListener, WriteAheadLogEntry, VolatileLocalState, CommitStream, write_ahead_log};
use crate::server;
use crate::actor::WeakActorClient;
use std::time::Duration;
use crate::commitlog::Log;
use crate::replica::election::{ElectionConfig, ElectionState};

pub(crate) fn create_replica<L>(
    logger: slog::Logger,
    cluster_tracker: ClusterTracker, // TODO:1 create internal
    commit_log: L,
    server_shutdown_handle: server::RpcServerShutdownHandle,
    actor_client: WeakActorClient,
    leader_heartbeat_duration: Duration,
    follower_min_timeout: Duration,
    follower_max_timeout: Duration,
    append_entries_timeout: Duration,
) -> (
    Replica<L>,
    CommitStream,
    ElectionStateChangeListener,
) where
    L: Log<WriteAheadLogEntry> + 'static,
{
    let my_replica_id = cluster_tracker.my_replica_id().clone();
    let (election_state, election_state_change_listener) = ElectionState::new_follower(
        ElectionConfig {
            my_replica_id: my_replica_id.clone(),
            leader_heartbeat_duration,
            follower_min_timeout,
            follower_max_timeout,
        },
        actor_client.clone(),
    );

    let (write_ahead_log, commit_stream) = write_ahead_log::wired(logger.clone(), commit_log);

    let local_state = Box::new(VolatileLocalState::new(my_replica_id.clone()));

    let replica = Replica::new(
        logger,
        my_replica_id,
        cluster_tracker,
        local_state,
        election_state,
        write_ahead_log,
        actor_client,
        append_entries_timeout,
        server_shutdown_handle,
    );

    (replica, commit_stream, election_state_change_listener)
}
