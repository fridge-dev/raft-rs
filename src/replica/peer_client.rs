use crate::grpc::grpc_raft_client::GrpcRaftClient;
use crate::grpc::{ProtoAppendEntriesReq, ProtoAppendEntriesResult, ProtoRequestVoteReq, ProtoRequestVoteResult};
use tonic::transport::{Channel, Endpoint, Uri};

#[derive(Clone)]
pub struct RaftClient {
    logger: slog::Logger,
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
    pub async fn new(logger: slog::Logger, uri: Uri) -> Self {
        let endpoint = Endpoint::from(uri);
        let connection = Self::try_connect(&logger, &endpoint).await;

        RaftClient {
            logger,
            endpoint,
            connection,
        }
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
            self.connection = Self::try_connect(&self.logger, &self.endpoint).await;
        } else {
            slog::debug!(self.logger, "Connection re-used");
        }
    }

    async fn try_connect(logger: &slog::Logger, endpoint: &Endpoint) -> Conn {
        match endpoint.connect().await {
            Ok(conn) => {
                slog::debug!(logger, "Successfully connected");
                Conn::Connected(GrpcRaftClient::new(conn))
            }
            Err(conn_err) => {
                slog::warn!(logger, "Failed to connect: {:?}", conn_err);
                Conn::Disconnected
            }
        }
    }
}
