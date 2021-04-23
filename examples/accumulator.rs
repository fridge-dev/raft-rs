use chrono::Utc;
use slog::Drain;
use std::fs::OpenOptions;
use std::net::Ipv4Addr;
use tokio;

#[tokio::main]
async fn main() {
    let cluster = fake_cluster();
    let logger = create_root_logger_for_stdout(cluster.my_replica_id.clone());
    let mut server = accumulator_impl::Accumulator::setup(cluster, logger)
        .await
        .expect("WTF");

    assert_eq!(100, server.add("k1".into(), 100).await.unwrap());
    assert_eq!(100, server.add("k2".into(), 100).await.unwrap());
    assert_eq!(-1, server.add("k1".into(), -101).await.unwrap());

    assert_eq!(-1, server.get("k1"));
    assert_eq!(100, server.get("k2"));
}

fn fake_cluster() -> raft::ClusterInfo {
    let raft_rpc_port = 2021;

    raft::ClusterInfo {
        my_replica_id: "id-1".into(),
        cluster_members: vec![
            raft::MemberInfo {
                replica_id: "id-1".into(),
                ip_addr: Ipv4Addr::from(0xFACE),
                raft_rpc_port,
                peer_redirect_info_blob: raft::MemberInfoBlob::new(1111),
            },
            raft::MemberInfo {
                replica_id: "id-2".into(),
                ip_addr: Ipv4Addr::from(0xBEEF),
                raft_rpc_port,
                peer_redirect_info_blob: raft::MemberInfoBlob::new(2222),
            },
            raft::MemberInfo {
                replica_id: "id-3".into(),
                ip_addr: Ipv4Addr::from(0x1337),
                raft_rpc_port,
                peer_redirect_info_blob: raft::MemberInfoBlob::new(3333),
            },
            raft::MemberInfo {
                replica_id: "id-4".into(),
                ip_addr: Ipv4Addr::from(0xDEAF),
                raft_rpc_port,
                peer_redirect_info_blob: raft::MemberInfoBlob::new(4444),
            },
            raft::MemberInfo {
                replica_id: "id-5".into(),
                ip_addr: Ipv4Addr::from(0xBEEB),
                raft_rpc_port,
                peer_redirect_info_blob: raft::MemberInfoBlob::new(5555),
            },
        ],
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
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    slog::Logger::root(drain, slog::o!("ReplicaId" => replica_id))
}

mod accumulator_impl {
    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use raft::RaftOptions;
    use std::collections::HashMap;
    use std::error::Error;

    pub struct Accumulator {
        replicated_log: raft::ReplicatedLog,
        commit_stream: raft::CommitStream,
        state_machine: AccumulatorStateMachine,
    }

    impl Accumulator {
        pub async fn setup(cluster_info: raft::ClusterInfo, logger: slog::Logger) -> Result<Self, Box<dyn Error>> {
            let client = raft::create_raft_client(raft::RaftClientConfig {
                commit_log_directory: "/raft".to_string(),
                info_logger: logger,
                cluster_info,
                options: RaftOptions::default(),
            })
            .await?;

            Ok(Accumulator {
                replicated_log: client.replication_log,
                commit_stream: client.commit_stream,
                state_machine: AccumulatorStateMachine::default(),
            })
        }

        pub async fn add(&mut self, key: String, value: i64) -> Result<i64, Box<dyn Error>> {
            // Phase 1 - enqueue for repl
            let data = encode_kv(key, value);
            let start_repl_input = raft::StartReplicationInput { data };
            let start_repl_output = self.replicated_log.start_replication(start_repl_input).await?;

            // Phase 2 - wait for commit and validate
            let committed_entry = self.commit_stream.next().await;
            assert_eq!(start_repl_output.key, committed_entry.key);
            let (key, value) = decode_kv(committed_entry.data);

            // Apply to state machine
            let applied_output = self.state_machine.add(key, value);
            Ok(applied_output)
        }

        pub fn get(&self, key: &str) -> i64 {
            self.state_machine.read(key)
        }
    }

    // Ideally this conversion should be represented in the lib's API?
    /// encode the key/value pair in the following way:
    /// | 8 bytes | variable length |
    /// |  value  |   key           |
    fn encode_kv(key: String, value: i64) -> Bytes {
        let mut bytes = BytesMut::with_capacity(8 + key.len());
        bytes.put_i64(value);
        bytes.put_slice(key.as_bytes());

        bytes.freeze()
    }

    fn decode_kv(mut bytes: Bytes) -> (String, i64) {
        let value = bytes.get_i64();
        let key = String::from_utf8_lossy(&bytes);

        (key.into_owned(), value)
    }

    #[derive(Default)]
    struct AccumulatorStateMachine {
        count_per_key: HashMap<String, i64>,
    }

    impl AccumulatorStateMachine {
        pub fn add(&mut self, key: String, value: i64) -> i64 {
            let mut new_value = value;
            if let Some(existing_value) = self.count_per_key.get(&key) {
                new_value += existing_value;
            }

            self.count_per_key.insert(key, new_value);

            new_value
        }

        pub fn read(&self, key: &str) -> i64 {
            *self.count_per_key.get(key).unwrap_or(&0)
        }
    }
}
