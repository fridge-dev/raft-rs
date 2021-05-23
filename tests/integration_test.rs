#![feature(assert_matches)]

use bytes::Bytes;
use chrono::Utc;
use raft;
use slog::Drain;
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::net::Ipv4Addr;
use tokio::time::{Duration, Instant};

#[tokio::test]
async fn leader_election_and_redirect() {
    let mut cluster = TestUtilClusterCreator {
        num_members: 5,
        port_base: 3000,
        heartbeat_duration: Duration::from_millis(500),
    }
    .create()
    .await;

    let leader_id = cluster.converge_on_leader_id(Duration::from_secs(10)).await;
    let non_leader_id = cluster.non_leader_id(&leader_id);

    // Try to find leader via redirect
    let output = cluster
        .client(&non_leader_id)
        .replication_log
        .start_replication(raft::StartReplicationInput { data: Bytes::default() })
        .await;

    let redirected_to = match output {
        Err(raft::StartReplicationError::LeaderRedirect { leader_id, .. }) => {
            println!("Redirected to {:?}", leader_id);
            leader_id
        }
        Ok(ok) => {
            panic!(
                "Initially discovered leader {:?}, then non-leader {:?} returned with OK: {:?}",
                leader_id, non_leader_id, ok
            );
        }
        Err(e) => {
            panic!("Failed to find leader: {:?}", e);
        }
    };
    assert_eq!(redirected_to, leader_id);

    // Confirm leader
    cluster
        .client(&leader_id)
        .replication_log
        .start_replication(raft::StartReplicationInput { data: Bytes::default() })
        .await
        .unwrap();
}

#[tokio::test]
async fn simple_commit() {
    let heartbeat_duration = Duration::from_millis(500);
    let mut cluster = TestUtilClusterCreator {
        num_members: 5,
        port_base: 4000,
        heartbeat_duration,
    }
    .create()
    .await;

    // Wait for leader election
    let leader_id = cluster.discover_leader_id(Duration::from_secs(10)).await;

    // Start repl of an entry
    let data_to_replicate = Bytes::from(String::from("Hello world"));
    let output = cluster
        .client(&leader_id)
        .replication_log
        .start_replication(raft::StartReplicationInput {
            data: data_to_replicate.clone(),
        })
        .await
        .unwrap();

    // Assert that all clients observe that the entry becomes committed.
    for (_, c) in cluster.clients.iter_mut() {
        let committed = c.commit_stream.next().await.unwrap();
        assert_eq!(committed.key, output.key);
        assert_eq!(committed.data, data_to_replicate);
    }

    // Wait before 2nd repl
    sleep(heartbeat_duration * 2).await;

    // Repl a 2nd entry
    let data_to_replicate = Bytes::from(String::from("it's me"));
    let output = cluster
        .client(&leader_id)
        .replication_log
        .start_replication(raft::StartReplicationInput {
            data: data_to_replicate.clone(),
        })
        .await
        .unwrap();

    // Assert that all clients observe that the entry becomes committed.
    for (_, c) in cluster.clients.iter_mut() {
        let committed = c.commit_stream.next().await.unwrap();
        assert_eq!(committed.key, output.key);
        assert_eq!(committed.data, data_to_replicate);
    }
}

#[tokio::test]
async fn graceful_shutdown() {
    let mut cluster = TestUtilClusterCreator {
        num_members: 5,
        port_base: 5000,
        heartbeat_duration: Duration::from_millis(500),
    }
    .create()
    .await;

    // Wait for leader election
    let leader_id = cluster.discover_leader_id(Duration::from_secs(10)).await;

    // Get non-leader
    let non_leader_id = cluster.non_leader_id(&leader_id);
    let non_leader_client = cluster.remove(&non_leader_id);
    let (replication_log, mut commit_stream, event_listener) = non_leader_client.destruct();

    // Kill replica actor. It is a local decision, so imagine other replicas can't know. The proper
    // way would be to leave the cluster, then kill the replica actor, but we haven't implemented
    // leave cluster operation.
    println!("Dropping {}", non_leader_id);
    drop(replication_log);

    // Assert termination and no panics
    assert_event_bus_closed(Duration::from_secs(3), event_listener).await;
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(3), commit_stream.next()).await,
        Ok(None),
        "commit_stream did not exit correctly"
    );
    // TODO:2 assert that host is not listening on port anymore.
}

async fn assert_event_bus_closed(timeout: Duration, mut event_listener: raft::EventListener) {
    let deadline = Instant::now() + timeout;
    loop {
        let event = tokio::time::timeout_at(deadline, event_listener.next_event())
            .await
            .expect("Timed out waiting for election event bus next()");
        match event {
            Some(s) => println!("Received {:?}", s),
            None => return,
        }
    }
}

// Experimenting with new pattern of separating factory methods from instance methods
// by using separate creator struct for creation.
struct TestUtilClusterCreator {
    pub num_members: usize,
    pub port_base: u16,
    pub heartbeat_duration: Duration,
}

impl TestUtilClusterCreator {
    pub async fn create(self) -> TestUtilCluster {
        let mut clients = HashMap::with_capacity(self.num_members);
        for i in 0..self.num_members {
            let client_config = Self::raft_config(i, self.num_members, self.port_base, self.heartbeat_duration);
            let client_id = client_config.cluster_info.my_replica_id.clone();
            let client = raft::create_raft_client(client_config).await.unwrap();
            clients.insert(client_id, client);
        }

        TestUtilCluster { clients }
    }

    fn raft_config(
        id: usize,
        num_members: usize,
        port_base: u16,
        heartbeat_duration: Duration,
    ) -> raft::RaftClientConfig {
        assert!(id < num_members, "ID must be in the range [0, {}]", num_members - 1);

        let mut cluster_members = Vec::with_capacity(num_members);
        for i in 0..num_members {
            cluster_members.push(Self::member_info(port_base, i));
        }

        let info_logger = create_root_logger_for_stdout(Self::repl_id(id));

        raft::RaftClientConfig {
            commit_log_directory: "/tmp/".to_string(),
            info_logger,
            cluster_info: raft::ClusterInfo {
                my_replica_id: Self::repl_id(id),
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
            replica_id: Self::repl_id(id),
            ip_addr: Ipv4Addr::from([127, 0, 0, 1]),
            raft_rpc_port: port_base + id as u16,
            peer_redirect_info_blob: raft::MemberInfoBlob::new(9200 + id as u128),
        }
    }

    fn repl_id(id: usize) -> String {
        format!("replica-{}", id + 1)
    }
}

struct TestUtilCluster {
    clients: HashMap<String, raft::RaftClient>,
}

impl TestUtilCluster {
    /// Wait for all clients to discover the same leader (but without Term validation)
    /// TODO:2 add Term to election event bus
    pub async fn converge_on_leader_id(&mut self, timeout: Duration) -> String {
        let deadline = Instant::now() + timeout;

        let mut leader_ids = HashSet::new();
        for (client_id, client) in self.clients.iter_mut() {
            leader_ids.insert(Self::wait_for_leader_id(client, client_id, deadline).await);
        }

        assert_eq!(1, leader_ids.len(), "Found >1 leader: {:?}", leader_ids);
        leader_ids.into_iter().next().unwrap()
    }

    /// Wait for any client to discover the leader
    pub async fn discover_leader_id(&mut self, timeout: Duration) -> String {
        let deadline = Instant::now() + timeout;
        let (any_client_id, any_client) = self.clients.iter_mut().next().unwrap();

        Self::wait_for_leader_id(any_client, any_client_id, deadline).await
    }

    async fn wait_for_leader_id(client: &mut raft::RaftClient, client_id: &str, deadline: Instant) -> String {
        loop {
            let event = tokio::time::timeout_at(deadline, client.event_listener.next_event())
                .await
                .expect("Timeout waiting for leader election")
                .expect("Expected election event bus to be alive");

            match event {
                raft::Event::Election(raft::ElectionEvent::Leader) => return client_id.to_string(),
                raft::Event::Election(raft::ElectionEvent::Follower(data)) => return data.leader_replica_id,
                raft::Event::Election(raft::ElectionEvent::Candidate) => { /* Continue */ }
                raft::Event::Election(raft::ElectionEvent::FollowerNoLeader) => { /* Continue */ }
            }
        }
    }

    pub fn non_leader_id(&self, leader_id: &str) -> String {
        let (non_leader_id, _) = self
            .clients
            .iter()
            .filter(|(client_id, _)| leader_id != *client_id)
            .next()
            .unwrap();
        non_leader_id.clone()
    }

    pub fn client(&self, client_id: &str) -> &raft::RaftClient {
        self.clients.get(client_id).unwrap()
    }

    pub fn remove(&mut self, client_id: &str) -> raft::RaftClient {
        self.clients.remove(client_id).unwrap()
    }
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
