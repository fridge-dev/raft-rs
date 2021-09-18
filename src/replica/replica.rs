use crate::actor::WeakActorClient;
use crate::commitlog::{Index, Log};
use crate::grpc::{
    proto_append_entries_error, proto_append_entries_result, proto_request_vote_error, proto_request_vote_result,
    ProtoAppendEntriesReq, ProtoAppendEntriesResult, ProtoRequestVoteReq,
};
use crate::replica::write_ahead_log::{WriteAheadLogEntry, WriteAheadLog};
use crate::replica::election::{
    ElectionState, ElectionStateSnapshot, PeerStateUpdate,
};
use crate::replica::local_state::{PersistentLocalState, Term};
use crate::replica::peer_client::PeerRpcClient;
use crate::replica::peers::{ClusterTracker, Peer, ReplicaId};
use crate::replica::replica_api::{
    AppendEntriesError, AppendEntriesInput, AppendEntriesOutput, AppendEntriesReplyFromPeer,
    EnqueueForReplicationError, EnqueueForReplicationInput, EnqueueForReplicationOutput, LeaderRedirectInfo,
    RequestVoteError, RequestVoteInput, RequestVoteOutput, RequestVoteReplyFromPeer, TermOutOfDateInfo,
};
use crate::replica::{
    AppendEntriesReplyFromPeerDescriptor, AppendEntriesReplyFromPeerError, LeaderTimerTick, RequestVoteResult,
};
use crate::server;
use std::collections::HashSet;
use std::{cmp, io};
use tokio::time::error::Elapsed;
use tokio::time::Duration;
use tonic::Status;

pub(crate) struct Replica<L> where L: Log<WriteAheadLogEntry> {
    logger: slog::Logger,
    my_replica_id: ReplicaId,
    cluster_tracker: ClusterTracker,
    local_state: Box<dyn PersistentLocalState + Send + Sync + 'static>,
    election_state: ElectionState,
    write_ahead_log: WriteAheadLog<L>,
    actor_client: WeakActorClient,
    append_entries_timeout: Duration,
    _server_shutdown: server::RpcServerShutdownHandle,
}

// TODO:1 Implement cluster membership changes.
// TODO:1 Implement log compaction.
impl<L> Replica<L> where L: Log<WriteAheadLogEntry> + 'static {
    pub(super) fn new(
        logger: slog::Logger,
        my_replica_id: ReplicaId,
        cluster_tracker: ClusterTracker,
        local_state: Box<dyn PersistentLocalState + Send + Sync + 'static>,
        election_state: ElectionState,
        write_ahead_log: WriteAheadLog<L>,
        actor_client: WeakActorClient,
        append_entries_timeout: Duration,
        server_shutdown: server::RpcServerShutdownHandle,
    ) -> Self {
        Self {
            logger,
            my_replica_id,
            cluster_tracker,
            local_state,
            election_state,
            write_ahead_log,
            actor_client,
            append_entries_timeout,
            _server_shutdown: server_shutdown,
        }
    }

    pub(crate) fn handle_enqueue_for_replication(
        &mut self,
        input: EnqueueForReplicationInput,
    ) -> Result<EnqueueForReplicationOutput, EnqueueForReplicationError> {
        // Leader check
        match self.election_state.current_state() {
            ElectionStateSnapshot::Leader => { /* carry on */ }
            ElectionStateSnapshot::Follower(leader_redir_info) => {
                return Err(EnqueueForReplicationError::LeaderRedirect(leader_redir_info));
            }
            ElectionStateSnapshot::Candidate => {
                return Err(EnqueueForReplicationError::NoLeader);
            }
            ElectionStateSnapshot::FollowerNoLeader => {
                return Err(EnqueueForReplicationError::NoLeader);
            }
        }

        // TODO:2.5 throttle based on number of uncommitted entries.

        // > If command received from client: append entry to local log,
        // > respond after entry applied to state machine (§5.3)
        let term = self.local_state.current_term();
        let new_entry = WriteAheadLogEntry {
            term,
            data: input.data.to_vec(),
        };
        let appended_index = self
            .write_ahead_log
            .append(new_entry)
            .map_err(|e| EnqueueForReplicationError::LocalIoError(e))?;

        // TODO:3 Eagerly trigger peer AppendEntries timers that are waiting.

        Ok(EnqueueForReplicationOutput {
            enqueued_term: term,
            enqueued_index: appended_index,
        })
    }

    pub(crate) fn server_handle_request_vote(
        &mut self,
        input: RequestVoteInput,
    ) -> Result<RequestVoteOutput, RequestVoteError> {
        // Ensure candidate is known member.
        if !self.cluster_tracker.contains_member(&input.candidate_id) {
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

    fn is_candidate_log_gte_mine(&self, candidate_last_entry: Option<(Term, Index)>) -> bool {
        // > Raft determines which of two logs is more up-to-date
        // > by comparing the index and term of the last entries in the
        // > logs. If the logs have last entries with different terms, then
        // > the log with the later term is more up-to-date. If the logs
        // > end with the same term, then whichever log is longer is
        // > more up-to-date.
        match (self.write_ahead_log.latest_entry(), candidate_last_entry) {
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

    pub(crate) fn handle_request_vote_reply_from_peer(&mut self, reply: RequestVoteReplyFromPeer) {
        let current_term = self.local_state.current_term();
        if current_term != reply.term {
            slog::info!(
                self.logger,
                "Received vote for outdated term {:?}, current term: {:?}.",
                reply.term,
                current_term,
            );
            return;
        }

        match reply.result {
            RequestVoteResult::VoteGranted => {
                let num_votes_received = match self.election_state.add_vote_if_candidate(reply.peer_id) {
                    Some(v) => v,
                    None => {
                        slog::info!(
                            self.logger,
                            "Received vote for term {:?} after transitioning to a election state: {:?}",
                            reply.term,
                            self.election_state,
                        );
                        return;
                    }
                };

                let num_voting_replicas = self.cluster_tracker.num_voting_replicas();
                slog::info!(
                    self.logger,
                    "Received {}/{} votes for term {:?}",
                    num_votes_received,
                    num_voting_replicas,
                    reply.term,
                );

                if num_votes_received >= Self::get_majority_vote_count(num_voting_replicas) {
                    self.election_state.transition_to_leader(
                        reply.term,
                        self.cluster_tracker.peer_ids(),
                        self.write_ahead_log.latest_entry().map(|(_, index)| index),
                    );
                }
            }
            RequestVoteResult::VoteNotGranted => {
                // No action
                slog::info!(
                    self.logger,
                    "Vote not granted from {:?} for term {:?}",
                    reply.peer_id,
                    reply.term,
                );
            }
            RequestVoteResult::RetryableFailure | RequestVoteResult::MalformedReply => {
                if let Some(peer) = self.cluster_tracker.peer(&reply.peer_id) {
                    // TODO:2.5 add RequestId to remote call
                    tokio::task::spawn(Self::call_peer_request_vote(
                        self.logger.clone(),
                        peer.client.clone(),
                        peer.metadata.replica_id().clone(),
                        self.new_request_vote_request(reply.term),
                        self.actor_client.clone(),
                        reply.term,
                    ));
                } else {
                    slog::error!(
                        self.logger,
                        "Peer {:?} not found while retrying RequestVote. Wtf!",
                        reply.peer_id
                    );
                }
            }
        }
    }

    fn get_majority_vote_count(num_voting_replicas: usize) -> usize {
        (num_voting_replicas / 2) + 1
    }

    pub(crate) fn server_handle_append_entries(
        &mut self,
        input: AppendEntriesInput,
    ) -> Result<AppendEntriesOutput, AppendEntriesError> {
        // Ensure candidate is known member. Note: This might be incorrect in context of membership
        // changes. Revisit then.
        let leader_redir_info = match self.cluster_tracker.metadata(&input.leader_id) {
            Some(md) => LeaderRedirectInfo {
                replica_id: md.replica_id().clone(),
                ip_addr: md.ip_addr(),
                replica_blob: md.info_blob(),
            },
            None => return Err(AppendEntriesError::ClientNotInCluster),
        };

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
            self.election_state.transition_to_follower(Some(leader_redir_info));
        } else {
            self.election_state.set_leader_if_unknown(&leader_redir_info);
        }

        // Reset follower timeout.
        // TODO:2 add logical clock to track if we receive a stale timer event.
        self.election_state.reset_timeout_if_follower();

        // 2. Reply false if [my] log doesn't contain an entry at [leader's]
        // prevLogIndex whose term matches [leader's] prevLogTerm (§5.3)
        if let Some((leader_prev_entry_term, leader_prev_entry_index)) = input.leader_previous_log_entry {
            match self.write_ahead_log.read(leader_prev_entry_index) {
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
        let mut next_entry_index = match &input.leader_previous_log_entry {
            None => Index::start_index(),
            Some((_, leader_prev_entry_index)) => leader_prev_entry_index.plus(1),
        };
        let mut last_appended_index = next_entry_index.checked_minus(1);
        for new_entry in input.new_entries.iter() {
            // TODO:2.5 optimize read to not happen if we know we've truncated log in a previous
            //          iteration, or if we're missing an entry from a previous iteration.
            let opt_existing_entry = self
                .write_ahead_log
                .read(next_entry_index)
                .map_err(|e| AppendEntriesError::ServerIoError(e))?;

            // 3. (if...)
            if let Some(existing_entry) = opt_existing_entry {
                if existing_entry.term == new_entry.term {
                    // 4. (no-op)
                    continue;
                } else {
                    // 3. (delete)
                    self.write_ahead_log
                        .truncate(next_entry_index)
                        .map_err(|e| AppendEntriesError::ServerIoError(e))?;
                }
            }

            // 4. (append)
            let appended_index = self
                .write_ahead_log
                .append(WriteAheadLogEntry {
                    term: new_entry.term,
                    data: new_entry.data.to_vec(),
                })
                .map_err(|e| AppendEntriesError::ServerIoError(e))?;
            assert_eq!(
                appended_index, next_entry_index,
                "Appended log entry to unexpected index."
            );

            next_entry_index = next_entry_index.plus(1);
            last_appended_index = Some(appended_index);
        }

        let computed_last_appended_index = match (input.leader_previous_log_entry, input.new_entries.len() as u64) {
            (None, 0) => None,
            (None, n) => Some(Index::new(n)),
            (Some((_, leader_prev_entry_index)), 0) => Some(leader_prev_entry_index),
            (Some((_, leader_prev_entry_index)), n) => Some(leader_prev_entry_index.plus(n)),
        };
        debug_assert_eq!(last_appended_index, computed_last_appended_index);

        // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        // TODO:1 Validate what "index of last new entry" means when new_entries is empty
        match (input.leader_commit_index, self.write_ahead_log.commit_index()) {
            (None, None) => { /* Nothing to do */ }
            (None, Some(my_commit_index)) => {
                // This is remotely possible if leader with same log as us, but didn't observe
                // latest commit index from previous leader, is elected.
                slog::warn!(
                    self.logger,
                    "My commit index ({:?}) is non-0 but leader ({:?})'s commit index is 0. Interesting...",
                    my_commit_index,
                    input.leader_id,
                );
            }
            (Some(leader_commit_index), None) => {
                let new_commit_index = match last_appended_index {
                    Some(my_last_index) => cmp::min(leader_commit_index, my_last_index),
                    // TODO:1 see above, I think this is wrong, it should be none?
                    None => leader_commit_index,
                };
                self.write_ahead_log.ratchet_fwd_commit_index_if_changed(new_commit_index);
            }
            (Some(leader_commit_index), Some(my_commit_index)) => {
                if leader_commit_index > my_commit_index {
                    let new_commit_index = match last_appended_index {
                        Some(my_last_index) => cmp::min(leader_commit_index, my_last_index),
                        None => leader_commit_index,
                    };
                    self.write_ahead_log.ratchet_fwd_commit_index_if_changed(new_commit_index);
                }
            }
        };

        let output = Ok(AppendEntriesOutput {});

        // > If commitIndex > lastApplied: increment lastApplied, apply
        // > log[lastApplied] to state machine (§5.3)
        self.write_ahead_log.apply_all_committed_entries();

        return output;
    }

    pub(crate) fn handle_append_entries_reply_from_peer(&mut self, reply: AppendEntriesReplyFromPeer) {
        let logger = self
            .logger
            .new(slog::o!("Peer" => format!("{:?}", reply.descriptor.peer_id), "SeqNo" => reply.descriptor.seq_no));
        slog::debug!(logger, "AE reply from peer result: {:?}", reply.result);

        if self.local_state.current_term() != reply.descriptor.term {
            slog::info!(
                logger,
                "Received AE reply for outdated term {:?}, but we're on term {:?}",
                reply.descriptor.term,
                self.local_state.current_term()
            );
            return;
        }

        match self.election_state.leader_state_mut() {
            None => slog::info!(logger, "No longer leader"),
            Some(leader_state) => {
                // 1. Check for stale term rejection
                let peer_log_update = match reply.result {
                    Err(AppendEntriesReplyFromPeerError::StaleTerm { new_term }) => {
                        slog::warn!(logger, "Rejected by peer because my term is stale.");
                        let increased = self.local_state.store_term_if_increased(new_term);
                        if increased {
                            self.election_state.transition_to_follower(None);
                            slog::info!(logger, "Transitioned to follower.");
                            return;
                        } else {
                            slog::warn!(logger, "This should not happen (unless peer has bug). Treating non-incrementing StaleTerm err as generic failure.");
                            PeerStateUpdate::OtherError
                        }
                    }
                    Err(AppendEntriesReplyFromPeerError::PeerMissingPreviousLogEntry) => {
                        slog::info!(logger, "Peer is missing previous log entry");
                        PeerStateUpdate::PeerLogBehind
                    }
                    Err(AppendEntriesReplyFromPeerError::RetryableFailure(err_msg)) => {
                        slog::error!(logger, "AE failure from remote peer: {:?}", err_msg);
                        // TODO:2 Consider adding exponential backoff w jitter here.
                        PeerStateUpdate::OtherError
                    }
                    Ok(_) => {
                        slog::info!(logger, "Successful AE reply");
                        PeerStateUpdate::Success {
                            previous_log_entry: reply.descriptor.previous_log_entry_index,
                            num_entries_replicated: reply.descriptor.num_log_entries,
                        }
                    }
                };

                // 2. Update peer log tracker
                let peer_state = match leader_state.peer_state_mut(&reply.descriptor.peer_id) {
                    None => {
                        slog::warn!(
                            logger,
                            "Peer {:?} not found while handling AE reply",
                            reply.descriptor.peer_id
                        );
                        return;
                    }
                    Some(peer_state) => peer_state,
                };
                peer_state.handle_append_entries_result(&logger, reply.descriptor.seq_no, peer_log_update);
                let (next_index, _) = peer_state.next_and_previous_log_index();

                // 3. Check for majority replication and apply new commits.
                // > If there exists an N such that N > commitIndex, a majority
                // > of matchIndex[i] ≥ N, and log[N].term == currentTerm:
                // > set commitIndex = N (§5.3, §5.4).
                // See also:
                // > Figure 8: A time sequence showing why a leader cannot determine
                // > commitment using log entries from older terms.
                let peers_matched_index: Vec<_> = leader_state
                    .peers_iter()
                    .map(|peer_state| peer_state.matched())
                    .collect();
                if let Some(tentative_new_commit_index) = Self::get_cluster_commit_index(peers_matched_index) {
                    match self
                        .write_ahead_log
                        .ratchet_fwd_commit_index_if_valid(tentative_new_commit_index, self.local_state.current_term())
                    {
                        Ok(_) => self.write_ahead_log.apply_all_committed_entries(),
                        Err(ioe) => slog::warn!(
                            logger,
                            "IO failure while confirming new commit index {:?}: {:?}",
                            tentative_new_commit_index,
                            ioe
                        ),
                    }
                }

                // 4. Enqueue next peer timer event.
                // > If last log index ≥ nextIndex for a follower: send
                // > AppendEntries RPC with log entries starting at nextIndex
                let mut do_immediate_call = false;
                if let Some(last_log_index) = self.write_ahead_log.latest_entry().map(|(_, v)| v) {
                    if last_log_index >= next_index {
                        do_immediate_call = true;
                    }
                }

                if do_immediate_call {
                    let actor_client = self.actor_client.clone();
                    let peer_heartbeat = LeaderTimerTick {
                        peer_id: reply.descriptor.peer_id,
                        term: reply.descriptor.term,
                    };
                    tokio::task::spawn(async move {
                        let _ = actor_client.leader_timer(peer_heartbeat).await;
                    });
                }
            }
        }
    }

    fn get_cluster_commit_index(mut peers_matched_indexes: Vec<Option<Index>>) -> Option<Index> {
        peers_matched_indexes.sort_by_key(|matched| match matched {
            None => 0u64,
            Some(m) => m.as_u64(),
        });

        // Overview of why algo is correct:
        // We are always at the tail of the array, because our log is same/longest.
        // 1. add "me"
        //let cluster_size = peers_matched_indexes.len() + 1;
        // 2. calculate majority
        //let majority = (cluster_size / 2) + 1;
        // 3. subtract "me"
        //let num_peers_to_achieve_majority = majority - 1;
        // 4. take `i`th index from the right
        //let quorum_idx = peers_matched_indexes.len() - num_peers_to_achieve_majority;

        // Or just use this simplified equation which is harder to understand at a glance why it
        // works. When in doubt, just read the unit tests.
        let quorum_idx = peers_matched_indexes.len() / 2;

        peers_matched_indexes.remove(quorum_idx)
    }

    pub(crate) fn handler_leader_timer(&mut self, input: LeaderTimerTick) {
        let current_term = self.local_state.current_term();
        if current_term != input.term {
            slog::warn!(
                self.logger,
                "Received leader heartbeat for outdated term {:?}, current term: {:?}",
                input.term,
                current_term
            );
            return;
        }

        let peer = match self.cluster_tracker.peer(&input.peer_id) {
            Some(peer) => peer.clone(),
            None => {
                slog::error!(self.logger, "Missing Peer {:?} in ClusterTracker", input.peer_id);
                return;
            }
        };

        match self.try_handle_leader_timer_for_peer(peer, current_term) {
            Ok(_) => {}
            Err(HandleLeaderTimerError::NoLongerLeader) => {
                slog::info!(self.logger, "Received leader timer event but no longer leader.")
            }
            Err(HandleLeaderTimerError::PeerConcurrencyThrottle) => {
                slog::warn!(self.logger, "Too many outstanding requests to peer {:?}", input.peer_id)
            }
            Err(HandleLeaderTimerError::DiskRead(index, ioe)) => {
                slog::error!(self.logger, "Failed to read log entry at index {:?}: {:?}", index, ioe);
            }
            Err(HandleLeaderTimerError::UnexpectedMissingLogEntry(index)) => {
                slog::error!(
                    self.logger,
                    "Wtf! LeaderStateTracker is tracking index {:?}, but entry is missing from log.",
                    index
                );
            }
            Err(HandleLeaderTimerError::LeaderStateMissingPeer {
                leader_state_tracker_peers,
            }) => {
                slog::error!(
                    self.logger,
                    "Wtf. Peer {:?} is present in ClusterTracker but missing in LeaderStateTracker. LeaderStateTracker peers: [{:?}]",
                    input.peer_id,
                    leader_state_tracker_peers,
                )
            }
        }
    }

    fn try_handle_leader_timer_for_peer(
        &mut self,
        peer: Peer,
        current_term: Term,
    ) -> Result<(), HandleLeaderTimerError> {
        match self.election_state.leader_state_mut() {
            None => Err(HandleLeaderTimerError::NoLongerLeader),
            Some(leader_state) => {
                let peer_state = match leader_state.peer_state_mut(peer.metadata.replica_id()) {
                    Some(ps) => ps,
                    None => {
                        return Err(HandleLeaderTimerError::LeaderStateMissingPeer {
                            leader_state_tracker_peers: leader_state.peer_ids(),
                        })
                    }
                };

                let (proto_request, descriptor) = leader_timer_handler::new_append_entries_request(
                    current_term,
                    self.my_replica_id.clone(),
                    peer.metadata.replica_id().clone(),
                    peer_state,
                    &self.write_ahead_log,
                )?;

                // TODO:2.5 add RequestId to remote call
                // TODO:2.5 change to use `async move` and put methods on peer or peer.client
                tokio::task::spawn(Self::call_peer_append_entries(
                    self.logger.clone(),
                    peer.client,
                    proto_request,
                    self.append_entries_timeout,
                    self.actor_client.clone(),
                    descriptor,
                ));

                peer_state.reset_heartbeat_timer();

                Ok(())
            }
        }
    }

    async fn call_peer_append_entries(
        logger: slog::Logger,
        mut peer_client: PeerRpcClient,
        rpc_request: ProtoAppendEntriesReq,
        rpc_timeout: Duration,
        callback: WeakActorClient,
        descriptor: AppendEntriesReplyFromPeerDescriptor,
    ) {
        slog::debug!(logger, "ClientWire - {:?}", rpc_request);
        let rpc_reply = tokio::time::timeout(rpc_timeout, peer_client.append_entries(rpc_request)).await;
        slog::debug!(logger, "ClientWire - {:?}", rpc_reply);

        let callback_input = AppendEntriesReplyFromPeer {
            descriptor,
            result: Self::convert_append_entries_rpc_reply(rpc_reply),
        };

        let _ = callback.notify_append_entries_reply_from_peer(callback_input).await;
    }

    fn convert_append_entries_rpc_reply(
        rpc_reply: Result<Result<ProtoAppendEntriesResult, Status>, Elapsed>,
    ) -> Result<(), AppendEntriesReplyFromPeerError> {
        match rpc_reply {
            Ok(Ok(rpc_result)) => match rpc_result.result {
                Some(proto_append_entries_result::Result::Ok(_)) => Ok(()),
                Some(proto_append_entries_result::Result::Err(err)) => {
                    match err.err {
                        Some(proto_append_entries_error::Err::ServerFault(payload)) => {
                            Err(AppendEntriesReplyFromPeerError::RetryableFailure(format!(
                                "Explicit server fault: {:?}",
                                payload.message
                            )))
                        }
                        Some(proto_append_entries_error::Err::StaleTerm(payload)) => {
                            Err(AppendEntriesReplyFromPeerError::StaleTerm {
                                new_term: Term::new(payload.current_term),
                            })
                        }
                        Some(proto_append_entries_error::Err::MissingLog(_)) => {
                            Err(AppendEntriesReplyFromPeerError::PeerMissingPreviousLogEntry)
                        }
                        Some(proto_append_entries_error::Err::ClientNotInCluster(_)) => {
                            // Retry in case peer is out of date. Not expecting this in practice.
                            Err(AppendEntriesReplyFromPeerError::RetryableFailure(
                                "Peer doesn't think we're in the cluster. Wtf?".into(),
                            ))
                        }
                        None => Err(AppendEntriesReplyFromPeerError::RetryableFailure(
                            "Malformed AppendEntries Err".into(),
                        )),
                    }
                }
                None => Err(AppendEntriesReplyFromPeerError::RetryableFailure(
                    "Malformed AppendEntries Result".into(),
                )),
            },
            Ok(Err(rpc_status)) => Err(AppendEntriesReplyFromPeerError::RetryableFailure(format!(
                "Un-modeled failure from AppendEntries RPC call: {:?}",
                rpc_status
            ))),
            Err(_timeout) => Err(AppendEntriesReplyFromPeerError::RetryableFailure(
                "Timed out calling AppendEntries".into(),
            )),
        }
    }

    pub(crate) fn handle_follower_timeout(&mut self) {
        // Write-ahead log style: Vote for self on local state before transitioning to candidate.
        let new_term = self.local_state.increment_term_and_vote_for_self();
        self.election_state.transition_to_candidate_and_vote_for_self();
        slog::info!(
            self.logger,
            "Timed out as follower. Changed to candidate. Election state: {:?}",
            self.election_state,
        );

        for peer in self.cluster_tracker.iter_peers() {
            // TODO:2.5 add RequestId to remote call
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
        let (last_log_entry_term, last_log_entry_index) = match self.write_ahead_log.latest_entry() {
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
        mut peer_client: PeerRpcClient,
        peer_id: ReplicaId,
        rpc_request: ProtoRequestVoteReq,
        callback: WeakActorClient,
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

        let _ = callback.notify_request_vote_reply_from_peer(callback_input).await;
    }
}

enum HandleLeaderTimerError {
    NoLongerLeader,
    PeerConcurrencyThrottle,
    DiskRead(Index, io::Error),
    UnexpectedMissingLogEntry(Index),
    LeaderStateMissingPeer {
        leader_state_tracker_peers: HashSet<ReplicaId>,
    },
}

mod leader_timer_handler {
    use crate::commitlog::{Index, Log};
    use crate::grpc::{ProtoAppendEntriesReq, ProtoLogEntry};
    use crate::replica::write_ahead_log::{WriteAheadLog, WriteAheadLogEntry};
    use crate::replica::election::PeerState;
    use crate::replica::replica::HandleLeaderTimerError;
    use crate::replica::{AppendEntriesReplyFromPeerDescriptor, ReplicaId, Term};

    pub(super) fn new_append_entries_request<L>(
        current_term: Term,
        my_id: ReplicaId,
        peer_id: ReplicaId,
        peer_state: &mut PeerState,
        commit_log: &WriteAheadLog<L>,
    ) -> Result<(ProtoAppendEntriesReq, AppendEntriesReplyFromPeerDescriptor), HandleLeaderTimerError>
    where
        L: Log<WriteAheadLogEntry>,
    {
        // Simplicity vs throughput tradeoff. We're just going to allow 1 outstanding request per
        // peer; no pipelining. This should not limit throughput too badly however, as we will still
        // batch log entries. This will support my target use case of a low-volume (~1k RPS) service.
        if peer_state.has_outstanding_request() {
            return Err(HandleLeaderTimerError::PeerConcurrencyThrottle);
        }
        let seq_no = peer_state.next_seq_no();

        let (next_index, opt_previous_index) = peer_state.next_and_previous_log_index();
        let opt_previous_log_entry_metadata = match opt_previous_index {
            None => None,
            Some(previous_index) => match commit_log.read(previous_index) {
                Ok(Some(entry)) => Some((entry.term, previous_index)),
                Ok(None) => return Err(HandleLeaderTimerError::UnexpectedMissingLogEntry(previous_index)),
                Err(e) => return Err(HandleLeaderTimerError::DiskRead(previous_index, e)),
            },
        };

        // Currently, we just send 1 new entry at a time.
        // TODO:2 send multiple entries in one request.
        let new_entries = match commit_log.read(next_index) {
            Ok(Some(entry)) => vec![entry],
            Ok(None) => Vec::new(),
            Err(e) => return Err(HandleLeaderTimerError::DiskRead(next_index, e)),
        };

        let descriptor = AppendEntriesReplyFromPeerDescriptor {
            peer_id,
            term: current_term,
            seq_no,
            previous_log_entry_index: opt_previous_index,
            num_log_entries: new_entries.len(),
        };

        let proto_request = build_append_entries_request(
            current_term,
            my_id,
            opt_previous_log_entry_metadata,
            commit_log.commit_index(),
            new_entries,
        );

        Ok((proto_request, descriptor))
    }

    // This is the infallible parts of creating the request object.
    fn build_append_entries_request(
        current_term: Term,
        my_id: ReplicaId,
        previous_log_entry_metadata: Option<(Term, Index)>,
        commit_index: Option<Index>,
        new_entries: Vec<WriteAheadLogEntry>,
    ) -> ProtoAppendEntriesReq {
        let commit_index_u64 = match commit_index {
            None => 0,
            Some(ci) => ci.as_u64(),
        };

        let (previous_log_entry_term_u64, previous_log_entry_index_u64) = match previous_log_entry_metadata {
            None => (0, 0),
            Some((term, idx)) => (term.as_u64(), idx.as_u64()),
        };

        let new_entries = new_entries
            .into_iter()
            .map(|entry| ProtoLogEntry {
                term: entry.term.as_u64(),
                data: entry.data,
            })
            .collect();

        ProtoAppendEntriesReq {
            client_node_id: my_id.into_inner(),
            term: current_term.as_u64(),
            commit_index: commit_index_u64,
            previous_log_entry_term: previous_log_entry_term_u64,
            previous_log_entry_index: previous_log_entry_index_u64,
            new_entries,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commitlog::{InMemoryLog, Index};
    use crate::replica::write_ahead_log::WriteAheadLogEntry;

    type Repl = Replica<InMemoryLog<WriteAheadLogEntry>>;

    fn opt_index(v: u64) -> Option<Index> {
        if v == 0 {
            None
        } else {
            Some(Index::new(v))
        }
    }

    #[test]
    fn test_commit_checker_logic() {
        fn run(expected: u64, matches: Vec<u64>) {
            let matches = matches.into_iter().map(|m| opt_index(m)).collect();

            let expected = opt_index(expected);

            assert_eq!(expected, Repl::get_cluster_commit_index(matches));
        }

        // 3-cluster
        run(0, vec![0, 0]);
        run(9, vec![0, 9]);
        run(9, vec![8, 9]);

        // 4-cluster
        run(0, vec![0, 0, 0]);
        run(0, vec![0, 0, 9]);
        run(8, vec![0, 8, 9]);
        run(8, vec![7, 8, 9]);

        // 5-cluster
        run(0, vec![0, 0, 0, 0]);
        run(0, vec![0, 0, 0, 9]);
        run(8, vec![0, 0, 8, 9]);
        run(8, vec![0, 7, 8, 9]);
        run(8, vec![6, 7, 8, 9]);

        // 6-cluster
        run(0, vec![0, 0, 0, 0, 0]);
        run(0, vec![0, 0, 0, 0, 9]);
        run(0, vec![0, 0, 0, 8, 9]);
        run(7, vec![0, 0, 7, 8, 9]);
        run(7, vec![0, 6, 7, 8, 9]);
        run(7, vec![5, 6, 7, 8, 9]);

        // 7-cluster
        run(0, vec![0, 0, 0, 0, 0, 0]);
        run(0, vec![0, 0, 0, 0, 0, 9]);
        run(0, vec![0, 0, 0, 0, 8, 9]);
        run(7, vec![0, 0, 0, 7, 8, 9]);
        run(7, vec![0, 0, 6, 7, 8, 9]);
        run(7, vec![0, 5, 6, 7, 8, 9]);
        run(7, vec![4, 5, 6, 7, 8, 9]);

        // Ordering doesn't matter
        run(9, vec![9, 8]);
        run(8, vec![7, 9, 8]);
        run(8, vec![6, 0, 8, 9]);
        run(7, vec![9, 8, 0, 0, 7]);
    }

    // fn create_root_logger_for_stdout(replica_id: String) -> slog::Logger {
    //     let decorator = slog_term::TermDecorator::new().build();
    //     let drain = slog_term::FullFormat::new(decorator).use_file_location().build().fuse();
    //     let drain = slog_async::Async::new(drain).build().fuse();
    //
    //     slog::Logger::root(drain, slog::o!("ReplicaId" => replica_id))
    // }
    //
    // struct ReplicaTestHandle {
    //     pub replica: Replica<InMemoryLog<LogEntry>, VolatileLocalState>,
    //     pub election_state_change_listener: ElectionStateChangeListener,
    //     pub commit_stream: RaftCommitStream,
    // }
    //
    // // TODO:0 WIP
    // #[test]
    // fn server_handle_append_entries() {
    //     let my_replica_id_str = String::from("repl-id-me");
    //     let my_replica_id = ReplicaId::new(&my_replica_id_str);
    //     let logger = create_root_logger_for_stdout(my_replica_id_str.clone());
    //
    //     let cluster_tracker = ClusterTracker::create_valid_cluster(
    //         logger.clone(),
    //         ReplicaMetadata::new(my_replica_id.clone(), Ipv4Addr::from(0x12345678), 0xABCD, ReplicaInfoBlob::new(0x12345678ABCD)),
    //         vec![
    //             ReplicaMetadata::new(ReplicaId::new("repl-id-peer1"), Ipv4Addr::from(0x1), 0x1, ReplicaInfoBlob::new(0x1)),
    //             ReplicaMetadata::new(ReplicaId::new("repl-id-peer2"), Ipv4Addr::from(0x2), 0x2, ReplicaInfoBlob::new(0x2)),
    //         ],
    //     ).await.unwrap();
    //
    //     let commit_log = InMemoryLog::create(logger.clone()).unwrap();
    //     let local_state = VolatileLocalState::new(my_replica_id.clone());
    //
    //     let (commit_stream_publisher, commit_stream) = api::create_commit_stream();
    //     let (actor_client, actor_queue_rx) = ActorClient::new(1);
    //
    //     let options = RaftOptionsValidated::try_from(config.options)?;
    //
    //     let (server_shutdown_handle, server_shutdown_signal) = server::shutdown_signal();
    //
    //     let (replica, election_state_change_listener) = Replica::new(ReplicaConfig {
    //         logger: logger.clone(),
    //         cluster_tracker,
    //         commit_log,
    //         local_state,
    //         commit_stream_publisher,
    //         server_shutdown_handle,
    //         actor_client: actor_client.weak(),
    //         leader_heartbeat_duration: options.leader_heartbeat_duration,
    //         follower_min_timeout: options.follower_min_timeout,
    //         follower_max_timeout: options.follower_max_timeout,
    //         append_entries_timeout: options.leader_append_entries_timeout,
    //     });
    // }
}
