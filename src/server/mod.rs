use crate::grpc::grpc_raft_server::{GrpcRaft, GrpcRaftServer};
use crate::grpc::{ProtoAppendEntriesReq, ProtoAppendEntriesResult, ProtoRequestVoteReq, ProtoRequestVoteResult, proto_request_vote_result, ProtoRequestVoteSuccess, ProtoClientNotInCluster, proto_append_entries_result, ProtoAppendEntriesSuccess, ProtoAppendEntriesError, proto_append_entries_error, ProtoClientStaleTerm, ProtoServerMissingPreviousLog, ProtoServerFault};
use tonic::{Request, Response, Status};
use crate::actor::ActorClient;
use crate::replica::{RequestVoteInput, RequestVoteOutput, RequestVoteError, AppendEntriesInput, AppendEntriesOutput, AppendEntriesError, Term, ReplicaId, AppendEntriesLogEntry};
use crate::commitlog::Index;
use bytes::Bytes;
use std::net::SocketAddr;
use tonic::transport::Server;

/// ServerAdapter is the type that implements the Raft gRPC interface.
pub struct ServerAdapter {
    local_replica: ActorClient,
}

impl ServerAdapter {
    pub fn new(local_replica: ActorClient) -> Self {
        ServerAdapter {
            local_replica,
        }
    }

    pub async fn run(self, socket_addr: SocketAddr) {
        println!("Going to listen on '{:?}'", socket_addr);

        // TODO:2 if server port is unavailable, signal back to caller.
        let result = Server::builder()
            .add_service(GrpcRaftServer::new(self))
            .serve(socket_addr)
            .await;

        println!("Raft gRPC server run() exited: {:?}", result);
    }
}

// associated methods
impl ServerAdapter {
    fn convert_request_vote_input(rpc_request: ProtoRequestVoteReq) -> RequestVoteInput {
        RequestVoteInput {
            candidate_term: Term::new(rpc_request.term),
            candidate_id: ReplicaId::new(rpc_request.client_node_id),
            candidate_last_log_entry_index: Index::new(rpc_request.last_log_entry_index),
            candidate_last_log_entry_term: Term::new(rpc_request.last_log_entry_term),
        }
    }

    fn convert_request_vote_result(app_result: Result<RequestVoteOutput, RequestVoteError>) -> ProtoRequestVoteResult {
        println!("Request vote result: {:?}", app_result);
        match app_result {
            Ok(ok) => {
                ProtoRequestVoteResult {
                    result: Some(proto_request_vote_result::Result::Ok(ProtoRequestVoteSuccess {
                        vote_granted: ok.vote_granted,
                    }))
                }
            }
            Err(RequestVoteError::CandidateNotInCluster) => {
                ProtoRequestVoteResult {
                    result: Some(proto_request_vote_result::Result::Ok(ProtoRequestVoteSuccess {
                        vote_granted: false,
                    }))
                }
            }
            Err(RequestVoteError::RequestTermOutOfDate(_)) => {
                ProtoRequestVoteResult {
                    result: Some(proto_request_vote_result::Result::Ok(ProtoRequestVoteSuccess {
                        vote_granted: false,
                    }))
                }
            }
        }
    }

    fn convert_append_entries_input(rpc_request: ProtoAppendEntriesReq) -> AppendEntriesInput {
        let mut new_entries = Vec::with_capacity(rpc_request.new_entries.len());
        for proto_entry in rpc_request.new_entries {
            new_entries.push(AppendEntriesLogEntry {
                term: Term::new(proto_entry.term),
                data: Bytes::from(proto_entry.data),
            })
        }

        AppendEntriesInput {
            leader_term: Term::new(rpc_request.term),
            leader_id: ReplicaId::new(rpc_request.client_node_id),
            leader_commit_index: Index::new(rpc_request.commit_index),
            new_entries,
            leader_previous_log_entry_index: Index::new(rpc_request.previous_log_entry_index),
            leader_previous_log_entry_term: Term::new(rpc_request.previous_log_entry_term),
        }
    }

    fn convert_append_entries_result(app_result: Result<AppendEntriesOutput, AppendEntriesError>) -> ProtoAppendEntriesResult {
        if let Err(e) = &app_result {
            println!("Append entries error: {:?}", e);
        }

        match app_result {
            Ok(_) => {
                ProtoAppendEntriesResult {
                    result: Some(proto_append_entries_result::Result::Ok(ProtoAppendEntriesSuccess {
                        // Empty
                    }))
                }
            }
            Err(AppendEntriesError::ClientNotInCluster) => {
                ProtoAppendEntriesResult {
                    result: Some(proto_append_entries_result::Result::Err(ProtoAppendEntriesError {
                        err: Some(proto_append_entries_error::Err::ClientNotInCluster(ProtoClientNotInCluster {
                            // Nothing
                        })),
                    })),
                }
            }
            Err(AppendEntriesError::ClientTermOutOfDate(term_info)) => {
                ProtoAppendEntriesResult {
                    result: Some(proto_append_entries_result::Result::Err(ProtoAppendEntriesError {
                        err: Some(proto_append_entries_error::Err::StaleTerm(ProtoClientStaleTerm {
                            current_term: term_info.current_term.into_inner(),
                        })),
                    })),
                }
            }
            Err(AppendEntriesError::ServerMissingPreviousLogEntry) => {
                ProtoAppendEntriesResult {
                    result: Some(proto_append_entries_result::Result::Err(ProtoAppendEntriesError {
                        err: Some(proto_append_entries_error::Err::MissingLog(ProtoServerMissingPreviousLog {
                            // Empty
                        })),
                    })),
                }
            }
            Err(AppendEntriesError::ServerIoError(_)) => {
                ProtoAppendEntriesResult {
                    result: Some(proto_append_entries_result::Result::Err(ProtoAppendEntriesError {
                        err: Some(proto_append_entries_error::Err::ServerFault(ProtoServerFault {
                            message: "Local IO failure".to_string(),
                        })),
                    })),
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl GrpcRaft for ServerAdapter {
    async fn request_vote(
        &self,
        rpc_request: Request<ProtoRequestVoteReq>,
    ) -> Result<Response<ProtoRequestVoteResult>, Status> {
        let app_input = Self::convert_request_vote_input(rpc_request.into_inner());
        let app_result = self.local_replica.request_vote(app_input).await;
        let rpc_reply = Self::convert_request_vote_result(app_result);
        Ok(Response::new(rpc_reply))
    }

    async fn append_entries(
        &self,
        rpc_request: Request<ProtoAppendEntriesReq>,
    ) -> Result<Response<ProtoAppendEntriesResult>, Status> {
        let app_input = Self::convert_append_entries_input(rpc_request.into_inner());
        let app_result = self.local_replica.append_entries(app_input).await;
        let rpc_reply = Self::convert_append_entries_result(app_result);
        Ok(Response::new(rpc_reply))
    }
}
