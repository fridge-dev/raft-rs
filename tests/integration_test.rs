use bytes::Bytes;
use chrono::Utc;
use raft;
use raft::RaftOptions;
use slog::Drain;
use std::collections::HashMap;
use std::error::Error;
use std::fs::OpenOptions;
use std::net::Ipv4Addr;
use tokio::time::Duration;

#[tokio::test]
async fn leader_election() -> Result<(), Box<dyn Error>> {
    let num_members = 5;
    let mut clients = HashMap::with_capacity(num_members);
    for i in 0..num_members {
        let client_config = config(i, 5);
        let client_id = client_config.cluster_info.my_replica_id.clone();
        let client = raft::create_raft_client(client_config).await?;
        clients.insert(client_id, client);
    }
    let clients = clients;

    sleep_sec(15).await;

    // Try to find leader
    let (_, any_client) = clients.iter().next().unwrap();
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

    sleep_sec(20).await;

    Ok(())
}

#[tokio::test]
async fn simple_commit() -> Result<(), Box<dyn Error>> {
    let num_members = 5;
    let mut clients = HashMap::with_capacity(num_members);
    for i in 0..num_members {
        let client_config = config(i, 5);
        let client_id = client_config.cluster_info.my_replica_id.clone();
        let client = raft::create_raft_client(client_config).await?;
        clients.insert(client_id, client);
    }

    // Wait for leader election
    sleep_sec(10).await;

    // Start repl of an entry
    let data_to_replicate = Bytes::from(String::from("Hello world"));
    let (_, any_client) = clients.iter_mut().next().unwrap();
    let result = any_client
        .replication_log
        .start_replication(raft::StartReplicationInput {
            data: data_to_replicate.clone(),
        })
        .await;

    let (_leader_client, output) = match result {
        Ok(ok) => (any_client, ok),
        Err(raft::StartReplicationError::LeaderRedirect { leader_id, .. }) => {
            println!("Redirected to {:?}", leader_id);
            let leader = clients.get_mut(&leader_id).unwrap();
            let output = leader
                .replication_log
                .start_replication(raft::StartReplicationInput {
                    data: data_to_replicate.clone(),
                })
                .await
                .unwrap();
            (leader, output)
        }
        Err(e) => panic!("Failed to find leader: {:?}", e),
    };

    // Assert that all clients observe that the entry becomes committed.
    for (_, c) in clients.iter_mut() {
        let committed = c.commit_stream.next().await;
        assert_eq!(committed.key, output.key);
        assert_eq!(committed.data, data_to_replicate);
    }

    Ok(())
}

fn config(id: usize, num_members: usize) -> raft::RaftClientConfig {
    assert!(id < num_members, "ID must be in the range [0, {}]", num_members - 1);

    let mut cluster_members = Vec::with_capacity(num_members);
    for i in 0..num_members {
        cluster_members.push(member_info(i));
    }

    let info_logger = create_root_logger_for_stdout(repl_id(id));

    raft::RaftClientConfig {
        commit_log_directory: "/tmp/".to_string(),
        info_logger,
        cluster_info: raft::ClusterInfo {
            my_replica_id: repl_id(id),
            cluster_members,
        },
        options: RaftOptions {
            leader_heartbeat_duration: Some(Duration::from_millis(3000)),
            follower_min_timeout: Some(Duration::from_millis(5000)),
            follower_max_timeout: Some(Duration::from_millis(10000)),
            ..RaftOptions::default()
        },
    }
}

fn member_info(id: usize) -> raft::MemberInfo {
    raft::MemberInfo {
        replica_id: repl_id(id),
        replica_ip_addr: Ipv4Addr::from([127, 0, 0, 1]),
        replica_port: 3000 + id as u16,
    }
}

fn repl_id(id: usize) -> String {
    format!("replica-{}", id + 1)
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

async fn sleep_sec(secs: u64) {
    println!("Sleep {} sec", secs);
    tokio::time::sleep(Duration::from_secs(secs)).await;
    println!("Awake!");
}
