use raft_rs::{MemberInfo, ClusterInfo};
use std::net::Ipv4Addr;

fn main() {
    let cluster = fake_cluster();
    let mut server = kv_app::KeyValueServer::setup(cluster).expect("WTF");

    server.put("k1".into(), "v1".into());
    server.put("k2".into(), "v2".into());
}

fn fake_cluster() -> ClusterInfo {
    ClusterInfo {
        my_replica_id: "id-1".into(),
        cluster_members: vec![
            MemberInfo {
                replica_id: "id-1".into(),
                replica_ip_addr: Ipv4Addr::from(0xFACE),
            },
            MemberInfo {
                replica_id: "id-2".into(),
                replica_ip_addr: Ipv4Addr::from(0xBEEF),
            },
            MemberInfo {
                replica_id: "id-3".into(),
                replica_ip_addr: Ipv4Addr::from(0x1337),
            },
            MemberInfo {
                replica_id: "id-4".into(),
                replica_ip_addr: Ipv4Addr::from(0xDEAF),
            },
            MemberInfo {
                replica_id: "id-5".into(),
                replica_ip_addr: Ipv4Addr::from(0xBEEB),
            },
        ],
    }
}

// This would technically go in another crate/repo, along with main. Soon, raft-rs repo won't have
// a main.
mod kv_app {
    use bytes::Bytes;
    use raft_rs::{LocalStateMachineApplier, RaftClientApi, RaftClientConfig, StateMachineOutput, WriteToLogInput, ClusterInfo};
    use std::collections::HashMap;
    use std::error::Error;

    #[derive(Default)]
    pub struct DumbStateMachine {
        key_value: HashMap<String, String>,
    }

    impl LocalStateMachineApplier for DumbStateMachine {
        fn apply_committed_entry(&mut self, bytes: Bytes) -> StateMachineOutput {
            let (key, value) = decode(bytes);
            self.key_value.insert(key, value);

            StateMachineOutput::NoData
        }
    }

    pub struct KeyValueServer {
        raft: Box<dyn RaftClientApi>,
    }

    impl KeyValueServer {
        pub fn setup(cluster_info: ClusterInfo) -> Result<Self, Box<dyn Error>> {
            let raft = raft_rs::create_raft_client(RaftClientConfig {
                state_machine: DumbStateMachine::default(),
                log_directory: "/raft/".to_string(),
                cluster_info
            })?;

            Ok(KeyValueServer { raft })
        }

        pub fn put(&mut self, key: String, value: String) {
            let bytes: Bytes = encode(key, value);
            let data: Vec<u8> = bytes.to_vec();
            self.raft.write_to_log(WriteToLogInput { data }).unwrap();
        }
    }

    fn encode(_key: String, _value: String) -> Bytes {
        unimplemented!()
    }

    fn decode(_bytes: Bytes) -> (String, String) {
        unimplemented!()
    }
}
