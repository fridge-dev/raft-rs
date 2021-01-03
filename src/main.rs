use raft_rs::{GrpcServer, ReplicaManager, InMemoryLogFactory};

fn main() {
    let replica_manager = ReplicaManager::new(
        "id-1".into(),
        InMemoryLogFactory::new(),
    );
    let server = GrpcServer::new(replica_manager);

    server.run();
}
