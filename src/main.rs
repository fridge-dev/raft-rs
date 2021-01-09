use raft_rs::{ClusterConfig, MemberInfo, ReplicaId};
use std::net::Ipv4Addr;

fn main() {
    let config = fake_cluster_config();
    let my_replica_id = config.cluster_members[0].id.clone().0;
    let mut server = kv_app::KeyValueServer::new(config, my_replica_id);

    server.put("k1".into(), "v1".into());
    server.put("k2".into(), "v2".into());
}

fn fake_cluster_config() -> ClusterConfig {
    ClusterConfig {
        cluster_members: vec![
            MemberInfo {
                id: ReplicaId("id-1".into()),
                ip: Ipv4Addr::from(0xFACE),
            },
            MemberInfo {
                id: ReplicaId("id-2".into()),
                ip: Ipv4Addr::from(0xBEEF),
            },
            MemberInfo {
                id: ReplicaId("id-3".into()),
                ip: Ipv4Addr::from(0x1337),
            },
            MemberInfo {
                id: ReplicaId("id-4".into()),
                ip: Ipv4Addr::from(0xDEAF),
            },
            MemberInfo {
                id: ReplicaId("id-5".into()),
                ip: Ipv4Addr::from(0xBEEB),
            },
        ],
    }
}

// This would technically go in another crate/repo, along with main. Soon, raft-rs repo won't have
// a main.
mod kv_app {
    use bytes::Bytes;
    use raft_rs::{
        ClusterConfig, LocalStateMachineApplier, RaftClientApi, RaftConfig, ReplicaId, StateMachineOutput,
        WriteToLogInput,
    };
    use std::collections::HashMap;

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
        pub fn new(cluster_config: ClusterConfig, my_replica_id: String) -> Self {
            let raft = raft_rs::create_raft_client(RaftConfig {
                state_machine: DumbStateMachine::default(),
                cluster_config,
                my_replica_id: ReplicaId(my_replica_id),
                log_directory: "/raft/".to_string(),
            });

            KeyValueServer { raft }
        }

        pub fn put(&mut self, key: String, value: String) {
            let bytes: Bytes = encode(key, value);
            let data: Vec<u8> = bytes.as_ref().to_vec();
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
