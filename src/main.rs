use raft_rs::{ClusterConfig, GrpcServer, MemberInfo, NoOpStateMachine, ReplicaId};
use std::net::Ipv4Addr;

fn main() {
    let config = fake_cluster_config();
    let my_replica_id = config.cluster_members[0].id.clone();
    let server = GrpcServer::new_local(config, my_replica_id, NoOpStateMachine::new());

    server.run();
}

fn fake_cluster_config() -> ClusterConfig {
    ClusterConfig {
        cluster_id: "helloworld".to_string(),
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
