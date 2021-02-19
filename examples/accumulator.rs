use bytes::{Buf, BufMut, Bytes, BytesMut};
use raft;
use std::collections::HashMap;
use std::error::Error;
use std::net::Ipv4Addr;
use tokio;

#[tokio::main]
async fn main() {
    let cluster = fake_cluster();
    let mut server = AccumulatorServer::setup(cluster).await.expect("WTF");

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

#[derive(Default)]
pub struct AccumulatorStateMachine {
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

impl raft::LocalStateMachineApplier for AccumulatorStateMachine {
    fn apply_committed_entry(&mut self, bytes: Bytes) -> raft::StateMachineOutput {
        let (key, value) = decode_kv(bytes);

        let new_value = self.add(key, value);

        raft::StateMachineOutput::Data(encode_v(new_value))
    }
}

pub struct AccumulatorServer {
    raft: Box<dyn raft::ReplicatedStateMachine<AccumulatorStateMachine>>,
}

impl AccumulatorServer {
    pub async fn setup(cluster_info: raft::ClusterInfo) -> Result<Self, Box<dyn Error>> {
        let raft = raft::create_raft_client(raft::RaftClientConfig {
            state_machine: AccumulatorStateMachine::default(),
            log_directory: "/raft/".to_string(),
            cluster_info,
        })
        .await?;

        Ok(AccumulatorServer { raft })
    }

    pub async fn add(&mut self, key: String, value: i64) -> Result<i64, Box<dyn Error>> {
        let data = encode_kv(key, value);
        let input = raft::WriteToLogApiInput { data };
        let output = self.raft.execute(input).await?;
        Ok(decode_v(output.applier_output))
    }

    pub fn get(&self, key: &str) -> i64 {
        self.raft.local_state_machine().read(key)
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

fn encode_v(value: i64) -> Bytes {
    let mut bytes = BytesMut::with_capacity(8);
    bytes.put_i64(value);

    bytes.freeze()
}

fn decode_v(mut bytes: Bytes) -> i64 {
    bytes.get_i64()
}
