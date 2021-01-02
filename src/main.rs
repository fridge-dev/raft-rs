use raft_rs::{GrpcServer, ReplicaManager, InMemoryLogFactory};
use std::net::Ipv4Addr;

fn main() {
    let replica_manager = ReplicaManager::new(
        Ipv4Addr::from(0xFACE),
        InMemoryLogFactory::new(),
    );
    let server = GrpcServer::new(replica_manager);

    server.run();
}
