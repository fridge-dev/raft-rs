use crate::commitlog::Index;
use crate::replica;
use crate::replica::{ReplicaInfoBlob, Term};
use std::net::Ipv4Addr;

// Opaque type for application to match CommittedEntry with.
#[derive(Debug, PartialEq)]
pub struct RaftEntryId {
    pub(crate) term: Term,
    pub(crate) entry_index: Index,
}

#[derive(Clone)]
pub struct RaftMemberInfo {
    pub replica_id: String,
    pub ip_addr: Ipv4Addr,
    pub raft_internal_rpc_port: u16,
    pub peer_redirect_info_blob: RaftMemberInfoBlob,
}

impl From<RaftMemberInfo> for replica::ReplicaMetadata {
    fn from(member_info: RaftMemberInfo) -> Self {
        Self::new(
            replica::ReplicaId::new(member_info.replica_id),
            member_info.ip_addr,
            member_info.raft_internal_rpc_port,
            replica::ReplicaInfoBlob::from(member_info.peer_redirect_info_blob),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftLeaderInfo {
    pub replica_id: String,
    pub ip: Ipv4Addr,
    pub info_blob: RaftMemberInfoBlob,
}

impl From<replica::LeaderRedirectInfo> for RaftLeaderInfo {
    fn from(internal_leader: replica::LeaderRedirectInfo) -> Self {
        Self {
            replica_id: internal_leader.replica_id.into_inner(),
            ip: internal_leader.ip_addr,
            info_blob: RaftMemberInfoBlob::from(internal_leader.replica_blob),
        }
    }
}

/// We allow application layer to provide an arbitrary blob of info about each member
/// that will be returned back to the application layer if we leader-redirect the
/// application to that member.
#[derive(Copy, Clone, Debug, Eq, PartialOrd, PartialEq)]
pub struct RaftMemberInfoBlob(u128);

impl RaftMemberInfoBlob {
    pub fn new(blob: u128) -> Self {
        RaftMemberInfoBlob(blob)
    }
}

impl From<RaftMemberInfoBlob> for replica::ReplicaInfoBlob {
    fn from(external_info_blob: RaftMemberInfoBlob) -> Self {
        Self::new(external_info_blob.0)
    }
}

impl From<replica::ReplicaInfoBlob> for RaftMemberInfoBlob {
    fn from(internal_info_blob: ReplicaInfoBlob) -> Self {
        Self::new(internal_info_blob.into_inner())
    }
}
