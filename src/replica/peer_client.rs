use crate::grpc::grpc_raft_client::GrpcRaftClient;
use crate::grpc::{ProtoAppendEntriesReq, ProtoAppendEntriesResult, ProtoRequestVoteReq, ProtoRequestVoteResult};
use tonic::transport::{Channel, Endpoint, Uri};

// This borderline doesn't need to exist, but I'm treating it as a way of limiting/simplifying the
// public API space of the generated gRPC client.
#[derive(Clone)]
pub(super) struct PeerRpcClient {
    client: GrpcRaftClient<Channel>,
}

impl PeerRpcClient {
    pub(super) fn new(uri: Uri) -> Self {
        let endpoint = Endpoint::from(uri);
        let channel = endpoint.connect_lazy().expect("infallible");
        let client = GrpcRaftClient::new(channel);

        PeerRpcClient { client }
    }

    pub(super) async fn request_vote(
        &mut self,
        request: ProtoRequestVoteReq,
    ) -> Result<ProtoRequestVoteResult, tonic::Status> {
        self.client.request_vote(request).await.map(|r| r.into_inner())
    }

    pub(super) async fn append_entries(
        &mut self,
        request: ProtoAppendEntriesReq,
    ) -> Result<ProtoAppendEntriesResult, tonic::Status> {
        self.client.append_entries(request).await.map(|r| r.into_inner())
    }
}
