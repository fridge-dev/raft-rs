#![allow(dead_code)]

mod api;
mod commitlog;
mod replica;
mod replica_manager;

pub use api::GrpcServer;
pub use commitlog::InMemoryLogFactory;
pub use replica::ReplicaId;
pub use replica_manager::ClusterConfig;
pub use replica_manager::ReplicaManager;

// Learning 1: `create::{root_mod}` should not have any code. Just `mod` and `pub use` statements.
// Learning 2: All `mod` statements, anywhere, should not be `pub`. Only export `pub` via individual
//             use statements.
//
// This keeps the `crate::{root_mod}` root_mod only responsible for exporting types to the rest of
// crate, and allows me to organize my root_mod impl however I want.
