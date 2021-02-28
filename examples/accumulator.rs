use std::net::Ipv4Addr;
use tokio;

#[tokio::main]
async fn main() {
    let cluster = fake_cluster();
    let mut server = accumulator_impl::Accumulator::setup(cluster).await.expect("WTF");

    assert_eq!(100, server.add("k1".into(), 100).await.unwrap());
    assert_eq!(100, server.add("k2".into(), 100).await.unwrap());
    assert_eq!(-1, server.add("k1".into(), -101).await.unwrap());

    assert_eq!(-1, server.get("k1"));
    assert_eq!(100, server.get("k2"));
}

fn fake_cluster() -> raft::ClusterInfo {
    let replica_port = 2021;

    raft::ClusterInfo {
        my_replica_id: "id-1".into(),
        cluster_members: vec![
            raft::MemberInfo {
                replica_id: "id-1".into(),
                replica_ip_addr: Ipv4Addr::from(0xFACE),
                replica_port,
            },
            raft::MemberInfo {
                replica_id: "id-2".into(),
                replica_ip_addr: Ipv4Addr::from(0xBEEF),
                replica_port,
            },
            raft::MemberInfo {
                replica_id: "id-3".into(),
                replica_ip_addr: Ipv4Addr::from(0x1337),
                replica_port,
            },
            raft::MemberInfo {
                replica_id: "id-4".into(),
                replica_ip_addr: Ipv4Addr::from(0xDEAF),
                replica_port,
            },
            raft::MemberInfo {
                replica_id: "id-5".into(),
                replica_ip_addr: Ipv4Addr::from(0xBEEB),
                replica_port,
            },
        ],
    }
}

mod accumulator_impl {
    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use std::collections::HashMap;
    use std::error::Error;
    use tokio::time::Duration;

    pub struct Accumulator {
        replicated_log: Box<dyn raft::ReplicatedLog>,
        commit_stream: raft::CommitStream,
        state_machine: AccumulatorStateMachine,
    }

    impl Accumulator {
        pub async fn setup(cluster_info: raft::ClusterInfo) -> Result<Self, Box<dyn Error>> {
            let client = raft::create_raft_client(raft::RaftClientConfig {
                log_directory: "/raft/".to_string(),
                cluster_info,
                leader_heartbeat_duration: Duration::from_millis(100),
                follower_min_timeout: Duration::from_millis(500),
                follower_max_timeout: Duration::from_millis(1500),
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
