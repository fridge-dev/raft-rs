use crate::grpc::grpc_raft_client::GrpcRaftClient;
use std::error::Error;
use std::net::Ipv4Addr;
use tonic::codegen::http::uri;
use tonic::transport::{Channel, Endpoint};

pub struct RaftClient {
    inner: GrpcRaftClient<Channel>,
}

impl RaftClient {
    pub async fn new(ip: Ipv4Addr, port: u16) -> Result<Self, ConnectError> {
        let ip_octets = ip.octets();
        let url = format!(
            "http://{}.{}.{}.{}:{}",
            ip_octets[0], ip_octets[1], ip_octets[2], ip_octets[3], port
        );
        println!("Connecting to {} ...", url);
        let endpoint = Endpoint::from_shared(url)?;

        let connection = endpoint.connect().await?;

        Ok(RaftClient {
            inner: GrpcRaftClient::new(connection),
        })
    }
}

pub enum ConnectError {
    InvalidUri(uri::InvalidUri),
    ConnectFailure(Box<dyn Error>),
}

impl From<uri::InvalidUri> for ConnectError {
    fn from(e: uri::InvalidUri) -> Self {
        ConnectError::InvalidUri(e)
    }
}

impl From<tonic::transport::Error> for ConnectError {
    fn from(e: tonic::transport::Error) -> Self {
        ConnectError::ConnectFailure(e.into())
    }
}
