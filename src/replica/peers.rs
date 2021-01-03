use std::net::Ipv4Addr;

/// ReplicaId...or maybe it should be NodeId or ServerId. Idk.
pub type ReplicaId = String;

#[derive(Clone)]
pub struct MemberInfo {
    pub id: ReplicaId,
    pub ip: Ipv4Addr,
}