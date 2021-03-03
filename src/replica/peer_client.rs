use crate::grpc::grpc_raft_client::GrpcRaftClient;
use crate::grpc::{ProtoAppendEntriesReq, ProtoAppendEntriesResult, ProtoRequestVoteReq, ProtoRequestVoteResult};
use tonic::transport::{Channel, Endpoint, Uri};

#[derive(Clone)]
pub struct RaftClient {
    endpoint: Endpoint,
    connection: Conn,
}

/// PeerConnection tracks the lifecycle of a connection. It enables us to start up our
/// replica even if we can't connect to others.
#[derive(Clone)]
enum Conn {
    Connected(GrpcRaftClient<Channel>),
    Disconnected,
}

impl RaftClient {
    pub async fn new(uri: Uri) -> Self {
        println!("Connecting to {:?} ...", uri);
        let endpoint = Endpoint::from(uri);
        let connection = Self::try_connect(&endpoint).await;

        RaftClient { endpoint, connection }
    }

    pub async fn request_vote(
        &mut self,
        request: ProtoRequestVoteReq,
    ) -> Result<ProtoRequestVoteResult, tonic::Status> {
        self.try_reconnect_if_needed().await;
        if let Conn::Connected(client) = &mut self.connection {
            return client.request_vote(request).await.map(|r| r.into_inner());
        }

        return Err(tonic::Status::new(tonic::Code::Unavailable, "couldn't connect"));
    }

    pub async fn append_entries(
        &mut self,
        request: ProtoAppendEntriesReq,
    ) -> Result<ProtoAppendEntriesResult, tonic::Status> {
        self.try_reconnect_if_needed().await;
        if let Conn::Connected(client) = &mut self.connection {
            return client.append_entries(request).await.map(|r| r.into_inner());
        }

        return Err(tonic::Status::new(tonic::Code::Unavailable, "couldn't connect"));
    }

    async fn try_reconnect_if_needed(&mut self) {
        if let Conn::Disconnected = self.connection {
            self.connection = Self::try_connect(&self.endpoint).await;
        }
    }

    async fn try_connect(endpoint: &Endpoint) -> Conn {
        match endpoint.connect().await {
            Ok(conn) => {
                println!("Successfully connected to {:?}", endpoint.uri());
                Conn::Connected(GrpcRaftClient::new(conn))
            }
            Err(conn_err) => {
                println!("Failed to connect to {:?} - {:?}", endpoint.uri(), conn_err);
                Conn::Disconnected
            }
        }
    }
}
