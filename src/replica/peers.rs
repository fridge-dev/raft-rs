use std::net::Ipv4Addr;

/// ReplicaId...or maybe it should be NodeId or ServerId. Idk.
#[derive(Clone)]
pub struct ReplicaId(pub String);

#[derive(Clone)]
pub struct MemberInfo {
    pub id: ReplicaId,
    pub ip: Ipv4Addr,
}
