use bytes::Bytes;
use chrono::Utc;
use raft;
use raft::ElectionStateSnapshot;
use slog::Drain;
use std::collections::HashMap;
use std::error::Error;
use std::fs::OpenOptions;
use std::net::Ipv4Addr;
use tokio::time::{Duration, Instant};

#[tokio::test]
async fn leader_election() -> Result<(), Box<dyn Error>> {
    let heartbeat_duration = Duration::from_millis(500);
    let num_members = 5;
    let mut clients = HashMap::with_capacity(num_members);
    for i in 0..num_members {
        let client_config = config(i, 5, 3000, heartbeat_duration);
        let client_id = client_config.cluster_info.my_replica_id.clone();
        let client = raft::create_raft_client(client_config).await?;
        clients.insert(client_id, client);
    }

    wait_for_leader_to_be_elected(&mut clients, Duration::from_secs(10)).await;

    let (_, any_client) = clients.iter().next().unwrap();

    // Try to find leader via redirect
    let output = any_client
        .replication_log
        .start_replication(raft::StartReplicationInput { data: Bytes::default() })
        .await;

    let leader = match output {
        Ok(ok) => {
            println!("Omg, got lucky and found leader. Replied: {:?}", ok);
            any_client
        }
        Err(raft::StartReplicationError::LeaderRedirect { leader_id, .. }) => {
            println!("Redirected to {:?}", leader_id);
            clients.get(&leader_id).unwrap()
        }
        Err(e) => {
            panic!("Failed to find leader: {:?}", e);
        }
    };

    // Confirm leader
    let output = leader
        .replication_log
        .start_replication(raft::StartReplicationInput { data: Bytes::default() })
        .await;
    if let Err(raft::StartReplicationError::LeaderRedirect { .. }) = output {
        panic!("wtf double redirect")
    }

    Ok(())
}

#[tokio::test]
async fn simple_commit() -> Result<(), Box<dyn Error>> {
    let num_members = 5;
    let heartbeat_duration = Duration::from_millis(500);
    let mut clients = HashMap::with_capacity(num_members);
    for i in 0..num_members {
        let client_config = config(i, 5, 4000, heartbeat_duration);
        let client_id = client_config.cluster_info.my_replica_id.clone();
        let client = raft::create_raft_client(client_config).await?;
        clients.insert(client_id, client);
    }

    // Wait for leader election
    let leader_id = discover_leader_id(&mut clients, Duration::from_secs(10)).await;

    // Start repl of an entry
    let data_to_replicate = Bytes::from(String::from("Hello world"));
    let output = clients
        .get(&leader_id)
        .expect("Leader missing!")
        .replication_log
        .start_replication(raft::StartReplicationInput {
            data: data_to_replicate.clone(),
        })
        .await
        .expect("Found incorrect leader");

    // Assert that all clients observe that the entry becomes committed.
    for (_, c) in clients.iter_mut() {
        let committed = c.commit_stream.next().await;
        assert_eq!(committed.key, output.key);
        assert_eq!(committed.data, data_to_replicate);
    }

    // Wait before 2nd repl
    sleep(heartbeat_duration * 2).await;

    // Repl a 2nd entry
    let data_to_replicate = Bytes::from(String::from("it's me"));
    let output = clients
        .get(&leader_id)
        .expect("Leader missing!")
        .replication_log
        .start_replication(raft::StartReplicationInput {
            data: data_to_replicate.clone(),
        })
        .await
        .expect("Found incorrect leader");

    // Assert that all clients observe that the entry becomes committed.
    for (_, c) in clients.iter_mut() {
        let committed = c.commit_stream.next().await;
        assert_eq!(committed.key, output.key);
        assert_eq!(committed.data, data_to_replicate);
    }

    Ok(())
}

fn config(id: usize, num_members: usize, port_base: u16, heartbeat_duration: Duration) -> raft::RaftClientConfig {
    assert!(id < num_members, "ID must be in the range [0, {}]", num_members - 1);

    let mut cluster_members = Vec::with_capacity(num_members);
    for i in 0..num_members {
        cluster_members.push(member_info(port_base, i));
    }

    let info_logger = create_root_logger_for_stdout(repl_id(id));

    raft::RaftClientConfig {
        commit_log_directory: "/tmp/".to_string(),
        info_logger,
        cluster_info: raft::ClusterInfo {
            my_replica_id: repl_id(id),
            cluster_members,
        },
        options: raft::RaftOptions {
            leader_heartbeat_duration: Some(heartbeat_duration),
            follower_min_timeout: Some(heartbeat_duration * 3),
            follower_max_timeout: Some(heartbeat_duration * 10),
            ..raft::RaftOptions::default()
        },
    }
}

fn member_info(port_base: u16, id: usize) -> raft::MemberInfo {
    raft::MemberInfo {
        replica_id: repl_id(id),
        ip_addr: Ipv4Addr::from([127, 0, 0, 1]),
        raft_rpc_port: port_base + id as u16,
        peer_redirect_info_blob: raft::MemberInfoBlob::new(4000 + id as u128),
    }
}

fn repl_id(id: usize) -> String {
    format!("replica-{}", id + 1)
}

async fn discover_leader_id(clients: &mut HashMap<String, raft::RaftClient>, timeout: Duration) -> String {
    let (any_client_id, any_client) = clients.iter_mut().next().unwrap();

    let deadline = Instant::now() + timeout;

    loop {
        let election_state = tokio::time::timeout_at(deadline, any_client.election_state_change_listener.next())
            .await
            .expect("Timeout waiting for leader election")
            .expect("Expected election event bus to be alive");

        match election_state {
            ElectionStateSnapshot::Leader => return any_client_id.clone(),
            // TODO:1 fix bad layer of abstraction, `ReplicaId`
            ElectionStateSnapshot::Follower(leader_id) => return leader_id.into_inner(),
            ElectionStateSnapshot::Candidate => { /* Continue */ }
            ElectionStateSnapshot::FollowerNoLeader => { /* Continue */ }
        }
    }
}

async fn wait_for_leader_to_be_elected(clients: &mut HashMap<String, raft::RaftClient>, timeout: Duration) {
    discover_leader_id(clients, timeout).await;
}

#[allow(dead_code)]
fn create_root_logger_for_file(directory_prefix: String, replica_id: String) -> slog::Logger {
    let now = Utc::now().format("%Y-%m-%dT%H:%M:%SZ");
    let log_path = format!("{}/info_log_{}/{}_info.log", directory_prefix, replica_id, now);
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(log_path)
        .unwrap();

    let decorator = slog_term::PlainDecorator::new(file);
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    slog::Logger::root(drain, slog::o!())
}

#[allow(dead_code)]
fn create_root_logger_for_stdout(replica_id: String) -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).use_file_location().build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    slog::Logger::root(drain, slog::o!("ReplicaId" => replica_id))
}

#[allow(dead_code)]
async fn sleep_sec(secs: u64) {
    sleep(Duration::from_secs(secs)).await;
}

async fn sleep(duration: Duration) {
    println!("Sleep {}ms", duration.as_millis());
    tokio::time::sleep(duration).await;
    println!("Awake!");
}
