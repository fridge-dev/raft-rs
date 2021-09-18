use crate::actor::WeakActorClient;
use crate::commitlog::Log;
use crate::replica::election::{ElectionConfig, ElectionState};
use crate::replica::local_state::VolatileLocalState;
use crate::replica::peers::{ClusterTracker, InvalidCluster};
use crate::replica::{
    write_ahead_log, CommitStream, ElectionStateChangeListener, Replica, ReplicaId, ReplicaMetadata, WriteAheadLogEntry,
};
use crate::server;
use std::time::Duration;

pub(crate) fn create_replica<L>(
    logger: slog::Logger,
    my_replica_id: ReplicaId,
    mut cluster_members: Vec<ReplicaMetadata>,
    commit_log: L,
    server_shutdown_handle: server::RpcServerShutdownHandle,
    actor_client: WeakActorClient,
    leader_heartbeat_duration: Duration,
    follower_min_timeout: Duration,
    follower_max_timeout: Duration,
    append_entries_timeout: Duration,
) -> Result<(Replica<L>, CommitStream, ElectionStateChangeListener), InvalidCluster>
where
    L: Log<WriteAheadLogEntry> + 'static,
{
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

    remove_self(&mut cluster_members, &my_replica_id);
    let cluster_tracker = ClusterTracker::create_valid_cluster(&my_replica_id, cluster_members)?;

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

    Ok((replica, commit_stream, election_state_change_listener))
}

fn remove_self(cluster_members: &mut Vec<ReplicaMetadata>, my_replica_id: &ReplicaId) {
    let mut to_remove = None;

    for (i, replica_metadata) in cluster_members.iter().enumerate() {
        if my_replica_id == replica_metadata.replica_id() {
            to_remove = Some(i);
            break;
        }
    }

    if let Some(i) = to_remove {
        cluster_members.remove(i);
    }
}
