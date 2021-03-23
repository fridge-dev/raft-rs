use crate::actor::ActorClient;
use crate::api;
use crate::commitlog::{Index, Log};
use crate::grpc::{
    proto_append_entries_error, proto_append_entries_result, proto_request_vote_error, proto_request_vote_result,
    ProtoAppendEntriesReq, ProtoRequestVoteReq,
};
use crate::replica::commit_log::{RaftCommitLogEntry, RaftLog};
use crate::replica::election::{CurrentLeader, ElectionConfig, ElectionState};
use crate::replica::local_state::{PersistentLocalState, Term};
use crate::replica::peer_client::RaftClient;
use crate::replica::peers::{PeerTracker, ReplicaId};
use crate::replica::replica_api::{
    AppendEntriesError, AppendEntriesInput, AppendEntriesOutput, AppendEntriesReplyFromPeer,
    EnqueueForReplicationError, EnqueueForReplicationInput, EnqueueForReplicationOutput, RequestVoteError,
    RequestVoteInput, RequestVoteOutput, RequestVoteReplyFromPeer, TermOutOfDateInfo,
};
use crate::replica::{LeaderLogState, RequestVoteResult};
use std::cmp;
use tokio::time::Duration;

pub struct ReplicaConfig<L, S>
where
    L: Log<RaftCommitLogEntry>,
    S: PersistentLocalState,
{
    pub peer_tracker: PeerTracker,
    pub commit_log: L,
    pub local_state: S,
    pub commit_stream_publisher: api::CommitStreamPublisher,
    pub actor_client: ActorClient,
    pub leader_heartbeat_duration: Duration,
    pub follower_min_timeout: Duration,
    pub follower_max_timeout: Duration,
    pub logger: slog::Logger,
}

pub struct Replica<L, S>
where
    L: Log<RaftCommitLogEntry>,
    S: PersistentLocalState,
{
    my_replica_id: ReplicaId,
    cluster_members: PeerTracker,
    local_state: S,
    election_state: ElectionState,
    commit_log: RaftLog<L>,
    actor_client: ActorClient,
    logger: slog::Logger,
}

impl<L, S> Replica<L, S>
where
    L: Log<RaftCommitLogEntry> + 'static,
    S: PersistentLocalState + 'static,
{
    pub fn new(config: ReplicaConfig<L, S>) -> Self {
        let my_replica_id = config.peer_tracker.my_replica_id().clone();
        let election_state = ElectionState::new_follower(
            ElectionConfig {
                my_replica_id: my_replica_id.clone(),
                leader_heartbeat_duration: config.leader_heartbeat_duration,
                follower_min_timeout: config.follower_min_timeout,
                follower_max_timeout: config.follower_max_timeout,
            },
            config.actor_client.clone(),
        );
        let commit_log = RaftLog::new(config.logger.clone(), config.commit_log, config.commit_stream_publisher);

        Replica {
            my_replica_id,
            cluster_members: config.peer_tracker,
            local_state: config.local_state,
            election_state,
            commit_log,
            actor_client: config.actor_client,
            logger: config.logger,
        }
    }

    pub fn handle_enqueue_for_replication(
        &mut self,
        input: EnqueueForReplicationInput,
    ) -> Result<EnqueueForReplicationOutput, EnqueueForReplicationError> {
        // Leader check
        match self.election_state.current_leader() {
            CurrentLeader::Me => { /* carry on */ }
            CurrentLeader::Other(leader_id) => match self.cluster_members.get_metadata(&leader_id) {
                Some(leader) => {
                    return Err(EnqueueForReplicationError::LeaderRedirect {
                        leader_id: leader_id.into_inner(),
                        leader_ip: leader.ip_addr(),
                        leader_port: leader.port(),
                    });
                }
                None => {
                    // This branch should technically be impossible.
                    // TODO:2.5 We can code-ify that by changing FollowerState to have IpAddr as well.
                    return Err(EnqueueForReplicationError::NoLeader);
                }
            },
            CurrentLeader::Unknown => {
                return Err(EnqueueForReplicationError::NoLeader);
            }
        }

        // > If command received from client: append entry to local log,
        // > respond after entry applied to state machine (§5.3)
        let term = self.local_state.current_term();
        let new_entry = RaftCommitLogEntry {
            term,
            data: input.data.to_vec(),
        };
        let appended_index = self
            .commit_log
            .append(new_entry)
            .map_err(|e| EnqueueForReplicationError::LocalIoError(e))?;

        Ok(EnqueueForReplicationOutput {
            enqueued_term: term,
            enqueued_index: appended_index,
        })
    }

    pub fn server_handle_request_vote(
        &mut self,
        input: RequestVoteInput,
    ) -> Result<RequestVoteOutput, RequestVoteError> {
        // Ensure candidate is known member.
        if !self.cluster_members.contains_member(&input.candidate_id) {
            return Err(RequestVoteError::CandidateNotInCluster);
        }

        // Read our local term/vote state as 1 atomic action.
        let (current_term, mut opt_voted_for) = self.local_state.voted_for_current_term();

        // 1. Reply false if term < currentTerm (§5.1)
        if input.candidate_term < current_term {
            slog::info!(self.logger, "Not granting vote. Client term is out of date.");
            return Err(RequestVoteError::RequestTermOutOfDate(TermOutOfDateInfo {
                current_term,
            }));
        }

        // > If RPC request or response contains term T > currentTerm:
        // > set currentTerm = T, convert to follower (§5.1)
        let increased = self.local_state.store_term_if_increased(input.candidate_term);
        if increased {
            self.election_state.transition_to_follower(None);
            slog::info!(
                self.logger,
                "Observed increased term in RequestVote call. Transitioning to follower. Election state: {:?}",
                self.election_state
            );
            // If we've increased the term, it means we haven't voted for anyone this term, and we will vote for the client.
            opt_voted_for = None;
        }

        // 2. If votedFor is null or candidateId, and candidate’s log is at
        // least as up-to-date as receiver’s log, grant vote (§5.2, §5.4).

        // If votedFor is null or candidateId, and...
        if let Some(voted_for) = opt_voted_for {
            if *voted_for != input.candidate_id {
                slog::info!(self.logger, "Not granting vote. We already voted for {:?}.", voted_for);
                return Ok(RequestVoteOutput { vote_granted: false });
            }
        }

        // ...and candidate’s log is at least as up-to-date as receiver’s log...
        if !self.is_candidate_log_gte_mine(input.candidate_last_log_entry) {
            slog::info!(self.logger, "Not granting vote. Candidate log is out of date.");
            return Ok(RequestVoteOutput { vote_granted: false });
        }

        // ...grant vote
        slog::info!(self.logger, "Voting for {:?}.", input.candidate_id);
        let cas_success = self
            .local_state
            .store_vote_for_term_if_unvoted(input.candidate_term, input.candidate_id.clone());

        if cas_success {
            return Ok(RequestVoteOutput { vote_granted: true });
        }

        // We lost CAS race. Re-read state and return success based on if previous winner
        // made the same vote as we would've.
        //
        // Note: In current in-memory impl of self.local_state, it is impossible we reach
        // here (simply due to `&mut self` in this method). This may change in the future
        // disk-based impl of self.local_state, and it's easy enough to handle it now, so
        // let's do it.
        if let (reread_current_term, Some(reread_voted_for)) = self.local_state.voted_for_current_term() {
            if reread_current_term == input.candidate_term && reread_voted_for.as_ref() == &input.candidate_id {
                // Client retried, we had 2 threads concurrently handling same vote.
                // Award vote to requester.
                return Ok(RequestVoteOutput { vote_granted: true });
            }
        }

        // If current state doesn't exactly match this request, for whatever
        // reason, don't grant vote.
        slog::info!(self.logger, "Not granting vote because idk why.");
        Ok(RequestVoteOutput { vote_granted: false })
    }

    // > Raft determines which of two logs is more up-to-date
    // > by comparing the index and term of the last entries in the
    // > logs. If the logs have last entries with different terms, then
    // > the log with the later term is more up-to-date. If the logs
    // > end with the same term, then whichever log is longer is
    // > more up-to-date.
    fn is_candidate_log_gte_mine(&self, candidate_last_entry: Option<(Term, Index)>) -> bool {
        match (self.commit_log.latest_entry(), candidate_last_entry) {
            (None, None) => true,
            (Some(_), None) => false,
            (None, Some(_)) => true,
            (
                Some((my_last_entry_term, my_last_entry_index)),
                Some((candidate_last_entry_term, candidate_last_entry_index)),
            ) => {
                if candidate_last_entry_term > my_last_entry_term {
                    return true;
                } else if candidate_last_entry_term < my_last_entry_term {
                    return false;
                }

                candidate_last_entry_index >= my_last_entry_index
            }
        }
    }

    pub fn handle_request_vote_reply_from_peer(&mut self, reply: RequestVoteReplyFromPeer) {
        match reply.result {
            RequestVoteResult::VoteGranted => {
                self.election_state
                    .add_vote_if_candidate_and_transition_to_leader_if_quorum(&self.logger, reply.term, reply.peer_id);
            }
            RequestVoteResult::VoteNotGranted => {
                // No action
            }
            RequestVoteResult::RetryableFailure | RequestVoteResult::MalformedReply => {
                if !self.election_state.is_currently_candidate_for_term(reply.term) {
                    return;
                }

                if let Some(peer) = self.cluster_members.peer(&reply.peer_id) {
                    // TODO:1.5 add RequestId to remote call
                    tokio::task::spawn(Self::call_peer_request_vote(
                        self.logger.clone(),
                        peer.client.clone(),
                        peer.metadata.replica_id().clone(),
                        self.new_request_vote_request(reply.term),
                        self.actor_client.clone(),
                        reply.term,
                    ));
                } else {
                    slog::warn!(
                        self.logger,
                        "Peer {:?} not found while retrying RequestVote. Wtf!",
                        reply.peer_id
                    );
                }
            }
        }
    }

    pub fn server_handle_append_entries(
        &mut self,
        input: AppendEntriesInput,
    ) -> Result<AppendEntriesOutput, AppendEntriesError> {
        // Ensure candidate is known member.
        if !self.cluster_members.contains_member(&input.leader_id) {
            return Err(AppendEntriesError::ClientNotInCluster);
        }

        // 1. Reply false if term < currentTerm (§5.1)
        let current_term = self.local_state.current_term();
        if input.leader_term < current_term {
            return Err(AppendEntriesError::ClientTermOutOfDate(TermOutOfDateInfo {
                current_term,
            }));
        }

        // > If RPC request or response contains term T > currentTerm:
        // > set currentTerm = T, convert to follower (§5.1)
        let increased = self.local_state.store_term_if_increased(input.leader_term);
        if increased {
            self.election_state
                .transition_to_follower(Some(input.leader_id.clone()));
        } else {
            self.election_state.set_leader_if_unknown(&input.leader_id);
        }

        // Reset follower timeout.
        // TODO:2 add logical clock to track if we receive a stale timer event.
        self.election_state.reset_timeout_if_follower();

        // 2. Reply false if [my] log doesn't contain an entry at [leader's]
        // prevLogIndex whose term matches [leader's] prevLogTerm (§5.3)
        if let Some((leader_prev_entry_term, leader_prev_entry_index)) = input.leader_log_state.previous_log_entry() {
            match self.commit_log.read(leader_prev_entry_index) {
                Ok(Some(my_previous_log_entry)) => {
                    if my_previous_log_entry.term != leader_prev_entry_term {
                        return Err(AppendEntriesError::ServerMissingPreviousLogEntry);
                    }
                }
                Ok(None) => return Err(AppendEntriesError::ServerMissingPreviousLogEntry),
                Err(e) => return Err(AppendEntriesError::ServerIoError(e)),
            };
        }

        // 3. If [my] existing entry conflicts with [leader's new entries]
        // (same index but different terms), delete [my] existing entry and
        // all that follow it (§5.3)
        // 4. Append any new entries not already in the log
        let mut next_entry_index = match input.leader_log_state.previous_log_entry() {
            None => Index::start_index(),
            Some((_, leader_prev_entry_index)) => leader_prev_entry_index.plus(1),
        };
        for new_entry in input.new_entries.iter() {
            // TODO:2 optimize read to not happen if we know we've truncated log in a previous iteration, or if we're missing an entry from a previous iteration.
            let opt_existing_entry = self
                .commit_log
                .read(next_entry_index)
                .map_err(|e| AppendEntriesError::ServerIoError(e))?;

            // 3. (if...)
            if let Some(existing_entry) = opt_existing_entry {
                if existing_entry.term == new_entry.term {
                    // 4. (no-op)
                    continue;
                } else {
                    // 3. (delete)
                    self.commit_log
                        .truncate(next_entry_index)
                        .map_err(|e| AppendEntriesError::ServerIoError(e))?;
                }
            }

            // 4. (append)
            let appended_index = self
                .commit_log
                .append(RaftCommitLogEntry {
                    term: new_entry.term,
                    data: new_entry.data.to_vec(),
                })
                .map_err(|e| AppendEntriesError::ServerIoError(e))?;
            assert_eq!(
                appended_index, next_entry_index,
                "Appended log entry to unexpected index."
            );

            next_entry_index = next_entry_index.plus(1);
        }

        // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        match input.leader_log_state {
            LeaderLogState::Normal {
                previous_log_entry_index,
                commit_index,
                ..
            } => {
                let index_of_last_new_entry = previous_log_entry_index.plus(input.new_entries.len() as u64);
                let new_commit_index = cmp::min(commit_index, index_of_last_new_entry);
                self.commit_log.ratchet_fwd_commit_index(new_commit_index);
            }
            LeaderLogState::Empty | LeaderLogState::NoCommit { .. } => {
                if let Some(my_commit_index) = self.commit_log.commit_index() {
                    panic!("My commit index ({:?}) is non-0 but leader ({:?})'s commit index is 0. This should be impossible.", my_commit_index, input.leader_id);
                }
            }
        }

        let output = Ok(AppendEntriesOutput {});

        // > If commitIndex > lastApplied: increment lastApplied, apply
        // > log[lastApplied] to state machine (§5.3)
        self.commit_log.apply_all_committed_entries();

        return output;
    }

    pub fn handle_append_entries_reply_from_peer(&mut self, reply: AppendEntriesReplyFromPeer) {
        slog::info!(self.logger, "AppendEntries result: {:?}", reply);

        // let appended_index = Index::new(1);
        // self.commit_log.ratchet_fwd_commit_index(appended_index);
        // self.commit_log.apply_all_committed_entries();
    }

    pub fn handle_leader_timer(&mut self) {
        // Check still leader
        if self.election_state.current_leader() != CurrentLeader::Me {
            return;
        }

        for peer in self.cluster_members.iter_peers() {
            tokio::task::spawn(Self::call_peer_append_entries(
                self.logger.clone(),
                peer.client.clone(),
                peer.metadata.replica_id().clone(),
                self.new_append_entries_request(),
                self.actor_client.clone(),
            ));
        }
    }

    fn new_append_entries_request(&self) -> ProtoAppendEntriesReq {
        let current_term = self.local_state.current_term().as_u64();
        let commit_index = match self.commit_log.commit_index() {
            None => 0,
            Some(ci) => ci.as_u64(),
        };
        let (previous_log_entry_term, previous_log_entry_index) = match self.commit_log.latest_entry() {
            None => (0, 0),
            Some((term, idx)) => (term.as_u64(), idx.as_u64()),
        };

        // TODO:1 Add locally buffered entries. For now, we're just doing empty heartbeats.
        let new_entries = Vec::new();

        ProtoAppendEntriesReq {
            client_node_id: self.my_replica_id.clone().into_inner(),
            term: current_term,
            commit_index,
            previous_log_entry_term,
            previous_log_entry_index,
            new_entries,
        }
    }

    async fn call_peer_append_entries(
        logger: slog::Logger,
        mut peer_client: RaftClient,
        peer_id: ReplicaId,
        rpc_request: ProtoAppendEntriesReq,
        callback: ActorClient,
    ) {
        slog::debug!(logger, "ClientWire - {:?}", rpc_request);
        let rpc_reply = peer_client.append_entries(rpc_request).await;
        slog::debug!(logger, "ClientWire - {:?}", rpc_reply);

        let fail = match rpc_reply {
            Ok(rpc_result) => match rpc_result.result {
                Some(proto_append_entries_result::Result::Ok(_)) => false,
                Some(proto_append_entries_result::Result::Err(err)) => {
                    match err.err {
                        Some(proto_append_entries_error::Err::ServerFault(payload)) => {
                            slog::warn!(logger, "Server returned failure: {:?}", payload.message);
                        }
                        Some(proto_append_entries_error::Err::StaleTerm(payload)) => {
                            slog::info!(
                                logger,
                                "Received outdated term failure. New term: {:?}",
                                payload.current_term
                            );
                        }
                        Some(proto_append_entries_error::Err::MissingLog(_)) => {
                            slog::info!(logger, "Server missing log.");
                        }
                        Some(proto_append_entries_error::Err::ClientNotInCluster(_)) => {
                            slog::info!(logger, "We're not in the cluster anymore.");
                        }
                        None => {
                            slog::warn!(logger, "Malformed AppendEntries reply");
                        }
                    }

                    true
                }
                None => {
                    slog::warn!(logger, "Malformed AppendEntries reply");
                    true
                }
            },
            Err(rpc_status) => {
                slog::warn!(
                    logger,
                    "Un-modeled failure from AppendEntries RPC call: {:?}",
                    rpc_status
                );
                true
            }
        };

        let callback_input = AppendEntriesReplyFromPeer { peer_id, fail };

        callback.notify_append_entries_reply_from_peer(callback_input).await;
    }

    pub fn handle_follower_timeout(&mut self) {
        let new_term = self.local_state.increment_term_and_vote_for_self();
        self.election_state
            .transition_to_candidate(new_term, self.cluster_members.num_voting_replicas());
        slog::info!(
            self.logger,
            "Timed out as follower. Changed to candidate. Election state: {:?}",
            self.election_state
        );

        for peer in self.cluster_members.iter_peers() {
            // TODO:1.5 add RequestId to remote call
            tokio::task::spawn(Self::call_peer_request_vote(
                self.logger.clone(),
                peer.client.clone(),
                peer.metadata.replica_id().clone(),
                self.new_request_vote_request(new_term),
                self.actor_client.clone(),
                new_term,
            ));
        }
    }

    fn new_request_vote_request(&self, term: Term) -> ProtoRequestVoteReq {
        let (last_log_entry_term, last_log_entry_index) = match self.commit_log.latest_entry() {
            None => (0, 0),
            Some((term, index)) => (term.as_u64(), index.as_u64()),
        };

        ProtoRequestVoteReq {
            client_node_id: self.my_replica_id.clone().into_inner(),
            term: term.as_u64(),
            last_log_entry_index,
            last_log_entry_term,
        }
    }

    async fn call_peer_request_vote(
        logger: slog::Logger,
        mut peer_client: RaftClient,
        peer_id: ReplicaId,
        rpc_request: ProtoRequestVoteReq,
        callback: ActorClient,
        term: Term,
    ) {
        slog::debug!(logger, "ClientWire - {:?}", rpc_request);
        let rpc_reply = peer_client.request_vote(rpc_request).await;
        slog::debug!(logger, "ClientWire - {:?}", rpc_reply);

        let callback_result = match rpc_reply {
            Ok(rpc_result) => match rpc_result.result {
                Some(proto_request_vote_result::Result::Ok(success_reply)) => {
                    if success_reply.vote_granted {
                        RequestVoteResult::VoteGranted
                    } else {
                        RequestVoteResult::VoteNotGranted
                    }
                }
                Some(proto_request_vote_result::Result::Err(err)) => match err.err {
                    Some(proto_request_vote_error::Err::ServerFault(fault)) => {
                        slog::warn!(logger, "RequestVote Service Fault: {:?}", fault.message);
                        RequestVoteResult::RetryableFailure
                    }
                    None => RequestVoteResult::MalformedReply,
                },
                None => RequestVoteResult::MalformedReply,
            },
            Err(rpc_status) => {
                slog::warn!(logger, "Un-modeled failure from RequestVote RPC call: {:?}", rpc_status);
                RequestVoteResult::RetryableFailure
            }
        };

        let callback_input = RequestVoteReplyFromPeer {
            peer_id,
            term,
            result: callback_result,
        };

        callback.notify_request_vote_reply_from_peer(callback_input).await;
    }
}
