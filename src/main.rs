use raft_rs::{GrpcServer, InMemoryLogFactory, ReplicaManager};

fn main() {
    let replica_manager = ReplicaManager::new(ReplicaId("id-1".into()), InMemoryLogFactory::new());
    let server = GrpcServer::new(replica_manager);

    server.run();
}
