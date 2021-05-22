use crate::actor::WeakActorClient;
use crate::commitlog::Index;
use crate::grpc::grpc_raft_server::{GrpcRaft, GrpcRaftServer};
use crate::grpc::{
    proto_append_entries_error, proto_append_entries_result, proto_request_vote_error, proto_request_vote_result,
    ProtoAppendEntriesError, ProtoAppendEntriesReq, ProtoAppendEntriesResult, ProtoAppendEntriesSuccess,
    ProtoClientNotInCluster, ProtoClientStaleTerm, ProtoRequestVoteError, ProtoRequestVoteReq, ProtoRequestVoteResult,
    ProtoRequestVoteSuccess, ProtoServerFault, ProtoServerMissingPreviousLog,
};
use crate::replica::{
    AppendEntriesError, AppendEntriesInput, AppendEntriesLogEntry, AppendEntriesOutput, ReplicaId, RequestVoteError,
    RequestVoteInput, RequestVoteOutput, Term,
};
use crate::server::RpcServerShutdownSignal;
use bytes::Bytes;
use std::net::SocketAddr;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

/// RpcServer is the type that implements the Raft gRPC interface.
pub struct RpcServer {
    logger: slog::Logger,
    local_replica: WeakActorClient,
}

impl RpcServer {
    pub fn new(logger: slog::Logger, local_replica: WeakActorClient) -> Self {
        RpcServer { logger, local_replica }
    }

    pub async fn run(self, socket_addr: SocketAddr, shutdown_signal: RpcServerShutdownSignal) {
        let logger = self.logger.clone();
        slog::info!(logger, "Listening on '{:?}'", socket_addr);

        // TODO:2 if server port is unavailable, signal back to caller.
        let result = Server::builder()
            .add_service(GrpcRaftServer::new(self))
            .serve_with_shutdown(socket_addr, shutdown_signal)
            .await;

        slog::info!(logger, "Server run() has exited: {:?}", result);
    }

    async fn handle_request_vote(&self, rpc_request: ProtoRequestVoteReq) -> Result<ProtoRequestVoteResult, Status> {
        let app_input = Self::convert_request_vote_input(rpc_request)?;
        let app_result = self.local_replica.request_vote(app_input).await;
        let rpc_reply = Self::convert_request_vote_result(app_result);
        Ok(rpc_reply)
    }

    fn convert_request_vote_input(rpc_request: ProtoRequestVoteReq) -> Result<RequestVoteInput, Status> {
        let candidate_last_log_entry =
            Self::convert_log_entry_metadata(rpc_request.last_log_entry_term, rpc_request.last_log_entry_index)?;

        Ok(RequestVoteInput {
            candidate_term: Term::new(rpc_request.term),
            candidate_id: ReplicaId::new(rpc_request.client_node_id),
            candidate_last_log_entry,
        })
    }

    fn convert_request_vote_result(app_result: Result<RequestVoteOutput, RequestVoteError>) -> ProtoRequestVoteResult {
        match app_result {
            Ok(ok) => ProtoRequestVoteResult {
                result: Some(proto_request_vote_result::Result::Ok(ProtoRequestVoteSuccess {
                    vote_granted: ok.vote_granted,
                })),
            },
            Err(RequestVoteError::CandidateNotInCluster) => ProtoRequestVoteResult {
                result: Some(proto_request_vote_result::Result::Ok(ProtoRequestVoteSuccess {
                    vote_granted: false,
                })),
            },
            Err(RequestVoteError::RequestTermOutOfDate(_)) => ProtoRequestVoteResult {
                result: Some(proto_request_vote_result::Result::Ok(ProtoRequestVoteSuccess {
                    vote_granted: false,
                })),
            },
            Err(RequestVoteError::ActorExited) => ProtoRequestVoteResult {
                result: Some(proto_request_vote_result::Result::Err(ProtoRequestVoteError {
                    err: Some(proto_request_vote_error::Err::ServerFault(ProtoServerFault {
                        message: "Server internal replica task has exited".to_string(),
                    })),
                })),
            },
        }
    }

    async fn handle_append_entries(
        &self,
        rpc_request: ProtoAppendEntriesReq,
    ) -> Result<ProtoAppendEntriesResult, Status> {
        let app_input = Self::convert_append_entries_input(rpc_request)?;
        let app_result = self.local_replica.append_entries(app_input).await;
        let rpc_reply = Self::convert_append_entries_result(app_result);
        Ok(rpc_reply)
    }

    fn convert_append_entries_input(rpc_request: ProtoAppendEntriesReq) -> Result<AppendEntriesInput, Status> {
        let leader_previous_log_entry = Self::convert_log_entry_metadata(
            rpc_request.previous_log_entry_term,
            rpc_request.previous_log_entry_index,
        )?;

        let leader_commit_index = Self::convert_log_commit_index(rpc_request.commit_index);

        let mut new_entries = Vec::with_capacity(rpc_request.new_entries.len());
        for proto_entry in rpc_request.new_entries {
            new_entries.push(AppendEntriesLogEntry {
                term: Term::new(proto_entry.term),
                data: Bytes::from(proto_entry.data),
            })
        }

        Ok(AppendEntriesInput {
            leader_term: Term::new(rpc_request.term),
            leader_id: ReplicaId::new(rpc_request.client_node_id),
            leader_previous_log_entry,
            leader_commit_index,
            new_entries,
        })
    }

    fn convert_log_entry_metadata(log_entry_term: u64, log_entry_index: u64) -> Result<Option<(Term, Index)>, Status> {
        match (log_entry_term, log_entry_index) {
            (0, 0) => Ok(None),
            (0, _) => {
                return Err(Status::invalid_argument(
                    "PreviousLogEntryTerm 0 and PreviousLogEntryIndex non-0",
                ))
            }
            (_, 0) => {
                return Err(Status::invalid_argument(
                    "PreviousLogEntryIndex 0 and PreviousLogEntryTerm non-0",
                ))
            }
            (term, index) => Ok(Some((Term::new(term), Index::new(index)))),
        }
    }

    fn convert_log_commit_index(log_commit_index: u64) -> Option<Index> {
        match log_commit_index {
            0 => None,
            index => Some(Index::new(index)),
        }
    }

    fn convert_append_entries_result(
        app_result: Result<AppendEntriesOutput, AppendEntriesError>,
    ) -> ProtoAppendEntriesResult {
        match app_result {
            Ok(_) => {
                ProtoAppendEntriesResult {
                    result: Some(proto_append_entries_result::Result::Ok(ProtoAppendEntriesSuccess {
                        // Empty
                    })),
                }
            }
            Err(AppendEntriesError::ClientNotInCluster) => {
                ProtoAppendEntriesResult {
                    result: Some(proto_append_entries_result::Result::Err(ProtoAppendEntriesError {
                        err: Some(proto_append_entries_error::Err::ClientNotInCluster(
                            ProtoClientNotInCluster {
                            // Nothing
                        },
                        )),
                    })),
                }
            }
            Err(AppendEntriesError::ClientTermOutOfDate(term_info)) => ProtoAppendEntriesResult {
                result: Some(proto_append_entries_result::Result::Err(ProtoAppendEntriesError {
                    err: Some(proto_append_entries_error::Err::StaleTerm(ProtoClientStaleTerm {
                        current_term: term_info.current_term.as_u64(),
                    })),
                })),
            },
            Err(AppendEntriesError::ServerMissingPreviousLogEntry) => {
                ProtoAppendEntriesResult {
                    result: Some(proto_append_entries_result::Result::Err(ProtoAppendEntriesError {
                        err: Some(proto_append_entries_error::Err::MissingLog(
                            ProtoServerMissingPreviousLog {
                            // Empty
                        },
                        )),
                    })),
                }
            }
            Err(AppendEntriesError::ServerIoError(_)) => ProtoAppendEntriesResult {
                result: Some(proto_append_entries_result::Result::Err(ProtoAppendEntriesError {
                    err: Some(proto_append_entries_error::Err::ServerFault(ProtoServerFault {
                        message: "Local IO failure".to_string(),
                    })),
                })),
            },
            Err(AppendEntriesError::ActorExited) => ProtoAppendEntriesResult {
                result: Some(proto_append_entries_result::Result::Err(ProtoAppendEntriesError {
                    err: Some(proto_append_entries_error::Err::ServerFault(ProtoServerFault {
                        message: "Server internal replica task has exited".to_string(),
                    })),
                })),
            },
        }
    }
}

#[async_trait::async_trait]
impl GrpcRaft for RpcServer {
    async fn request_vote(
        &self,
        rpc_request_wrapped: Request<ProtoRequestVoteReq>,
    ) -> Result<Response<ProtoRequestVoteResult>, Status> {
        let rpc_request = rpc_request_wrapped.into_inner();

        slog::debug!(self.logger, "ServerWire - {:?}", rpc_request);
        let rpc_result = self.handle_request_vote(rpc_request).await;
        slog::debug!(self.logger, "ServerWire - {:?}", rpc_result);

        rpc_result.map(Response::new)
    }

    async fn append_entries(
        &self,
        rpc_request_wrapped: Request<ProtoAppendEntriesReq>,
    ) -> Result<Response<ProtoAppendEntriesResult>, Status> {
        let rpc_request = rpc_request_wrapped.into_inner();

        slog::debug!(self.logger, "ServerWire - {:?}", rpc_request);
        let rpc_result = self.handle_append_entries(rpc_request).await;
        slog::debug!(self.logger, "ServerWire - {:?}", rpc_result);

        rpc_result.map(Response::new)
    }
}
