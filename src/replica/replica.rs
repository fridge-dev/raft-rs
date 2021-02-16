use crate::commitlog::{Index, Log};
use crate::replica::commit_log::{RaftLog, RaftLogEntry};
use crate::replica::election::{CurrentLeader, ElectionState};
use crate::replica::local_state::{PersistentLocalState, Term};
use crate::replica::peers::{Cluster, ReplicaId};
use crate::replica::raft_rpcs::{
    AppendEntriesError, AppendEntriesInput, AppendEntriesOutput, RaftRpcHandler, RequestVoteError, RequestVoteInput,
    RequestVoteOutput, TermOutOfDateInfo,
};
use crate::{LocalStateMachineApplier, StateMachineOutput, WriteToLogError, WriteToLogInput, WriteToLogOutput};
use bytes::Bytes;
use std::cmp;

pub struct ReplicaConfig<L, S, M>
where
    L: Log<RaftLogEntry>,
    S: PersistentLocalState,
    M: LocalStateMachineApplier,
{
    pub cluster: Cluster,
    pub log: L,
    pub local_state: S,
    pub state_machine: M,
}

pub struct Replica<L, S, M>
where
    L: Log<RaftLogEntry>,
    S: PersistentLocalState,
    M: LocalStateMachineApplier,
{
    my_replica_id: ReplicaId,
    cluster_members: Cluster,
    local_state: S,
    election_state: ElectionState,
    commit_log: RaftLog<L, M>,
}

impl<L, S, M> Replica<L, S, M>
where
    L: Log<RaftLogEntry>,
    S: PersistentLocalState,
    M: LocalStateMachineApplier,
{
    pub fn new(config: ReplicaConfig<L, S, M>) -> Self {
        let commit_log = RaftLog::new(config.log, config.state_machine);
        let my_replica_id = config.cluster.my_replica_id().clone();
        Replica {
            my_replica_id,
            cluster_members: config.cluster,
            local_state: config.local_state,
            election_state: ElectionState::new_follower(),
            commit_log,
        }
    }

    pub fn write_to_log(&mut self, input: WriteToLogInput) -> Result<WriteToLogOutput, WriteToLogError> {
        // Leader check
        match self.election_state.current_leader() {
            CurrentLeader::Me => { /* carry on */ }
            CurrentLeader::Other(leader_id) => match self.cluster_members.get_metadata(&leader_id) {
                Some(leader) => {
                    return Err(WriteToLogError::LeaderRedirect {
                        leader_id: leader_id.into_inner(),
                        leader_ip: leader.ip_addr(),
                        leader_port: leader.port(),
                    });
                }
                None => {
                    // This branch should technically be impossible.
                    // TODO:2.5 We can code-ify that by changing FollowerState to have IpAddr as well.
                    return Err(WriteToLogError::NoLeader);
                }
            },
            CurrentLeader::Unknown => {
                return Err(WriteToLogError::NoLeader);
            }
        }

        // > If command received from client: append entry to local log,
        // > respond after entry applied to state machine (§5.3)
        let term = self.local_state.current_term();
        let new_entry = RaftLogEntry {
            term,
            data: input.data.to_vec(),
        };
        let appended_index = self
            .commit_log
            .append(new_entry)
            .map_err(|e| WriteToLogError::LocalIoError(e))?;

        self.replicate_new_entry(appended_index, term, input.data.clone());

        self.commit_log.ratchet_fwd_commit_index(appended_index);
        let state_machine_output = self
            .commit_log
            .try_apply_all_committed_entries()
            .map_err(|e| WriteToLogError::LocalIoError(e))?
            // This should not happen, and indicates some erroneous state on the server.
            .expect("Incremented commit index but then there was no corresponding entry to apply.");

        let applier_output = match state_machine_output {
            StateMachineOutput::Data(outcome) => outcome,
            StateMachineOutput::NoData => Bytes::new(),
        };

        Ok(WriteToLogOutput { applier_output })
    }

    fn replicate_new_entry(&self, _entry_index: Index, _entry_term: Term, _entry_data: Bytes) {
        // TODO:2 (after gRPC setup and threading/timers) replicate entries
    }

    pub fn local_state_machine(&self) -> &M {
        self.commit_log.state_machine()
    }

    // > Raft determines which of two logs is more up-to-date
    // > by comparing the index and term of the last entries in the
    // > logs. If the logs have last entries with different terms, then
    // > the log with the later term is more up-to-date. If the logs
    // > end with the same term, then whichever log is longer is
    // > more up-to-date.
    fn is_candidate_more_up_to_date_than_me(
        &self,
        candidate_last_entry_term: Term,
        candidate_last_entry_index: Index,
    ) -> bool {
        if let Some((my_last_entry_term, my_last_entry_index)) = self.commit_log.latest_entry() {
            if candidate_last_entry_term > my_last_entry_term {
                return true;
            } else if candidate_last_entry_term < my_last_entry_term {
                return false;
            }

            candidate_last_entry_index > my_last_entry_index
        } else {
            true
        }
    }
}

impl<L, S, M> RaftRpcHandler for Replica<L, S, M>
where
    L: Log<RaftLogEntry>,
    S: PersistentLocalState,
    M: LocalStateMachineApplier,
{
    fn handle_request_vote(&mut self, input: RequestVoteInput) -> Result<RequestVoteOutput, RequestVoteError> {
        // Ensure candidate is known member.
        if !self.cluster_members.contains_member(&input.candidate_id) {
            return Err(RequestVoteError::CandidateNotInCluster);
        }

        // Read our local term/vote state as 1 atomic action.
        let (current_term, opt_voted_for) = self.local_state.voted_for_current_term();

        // 1. Reply false if term < currentTerm (§5.1)
        if input.candidate_term < current_term {
            return Err(RequestVoteError::RequestTermOutOfDate(TermOutOfDateInfo {
                current_term,
            }));
        }

        // > If RPC request or response contains term T > currentTerm:
        // > set currentTerm = T, convert to follower (§5.1)
        let increased = self.local_state.store_term_if_increased(input.candidate_term);
        if increased {
            self.election_state.transition_to_follower(None);
        }

        // 2. If votedFor is null or candidateId, and candidate’s log is at
        // least as up-to-date as receiver’s log, grant vote (§5.2, §5.4).

        // If votedFor is null or candidateId, and...
        let can_vote_for_candidate = match opt_voted_for {
            None => true,
            Some(voted_for) => *voted_for == input.candidate_id,
        };
        let already_voted_for_someone_else = !can_vote_for_candidate;
        if already_voted_for_someone_else {
            return Ok(RequestVoteOutput { vote_granted: false });
        }

        // ...and candidate’s log is at least as up-to-date as receiver’s log...
        if !self.is_candidate_more_up_to_date_than_me(
            input.candidate_last_log_entry_term,
            input.candidate_last_log_entry_index,
        ) {
            return Ok(RequestVoteOutput { vote_granted: false });
        }

        // ...grant vote
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
        Ok(RequestVoteOutput { vote_granted: false })
    }

    fn handle_append_entries(&mut self, input: AppendEntriesInput) -> Result<AppendEntriesOutput, AppendEntriesError> {
        // 0. Ensure candidate is known member.
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
        }

        // 2. Reply false if [my] log doesn't contain an entry at [leader's]
        // prevLogIndex whose term matches [leader's] prevLogTerm (§5.3)
        // TODO:2 readability improvement
        let me_missing_entry = match self.commit_log.read(input.leader_previous_log_entry_index) {
            Ok(Some(my_previous_log_entry)) => Ok(my_previous_log_entry.term != input.leader_previous_log_entry_term),
            Ok(None) => Ok(true),
            Err(e) => Err(AppendEntriesError::ServerIoError(e)),
        }?;
        if me_missing_entry {
            return Err(AppendEntriesError::ServerMissingPreviousLogEntry);
        }

        // 3. If [my] existing entry conflicts with [leader's] (same index
        // but different terms), delete [my] existing entry and all that
        // follow it (§5.3)
        // 4. Append any new entries not already in the log
        let mut new_entry_index = input.leader_previous_log_entry_index;
        for new_entry in input.new_entries.iter() {
            new_entry_index.incr(1);

            // TODO:2 optimize read to not happen if we know we've truncated log in a previous iteration.
            let opt_existing_entry = self
                .commit_log
                .read(new_entry_index)
                .map_err(|e| AppendEntriesError::ServerIoError(e))?;

            // 3. (if...)
            if let Some(existing_entry) = opt_existing_entry {
                if existing_entry.term == new_entry.term {
                    // 4. (no-op)
                    continue;
                } else {
                    // 3. (delete)
                    self.commit_log
                        .truncate(new_entry_index)
                        .map_err(|e| AppendEntriesError::ServerIoError(e))?;
                }
            }

            // 4. (append)
            self.commit_log
                .append(RaftLogEntry {
                    term: new_entry.term,
                    data: new_entry.data.to_vec(),
                })
                .map_err(|e| AppendEntriesError::ServerIoError(e))?;
        }

        // 5. If leaderCommit > commitIndex, set commitIndex =
        // min(leaderCommit, index of last new entry)
        if input.leader_commit_index > self.commit_log.commit_index() {
            // TODO:2 wtf to do if entries was empty?? Paper doesn't state this. Formal spec probably does though.
            //        For now, assuming it's safe to update. Eventually, read spec to confirm.
            //        Being pragmatic: Leader could say their commit index is N+1 despite they've only replicated
            //        indexes [_, N] to us. In which case, we should only mark N as committed and wait until we
            //        get sent N+1. If entries are missing, then make sure to not increment commit index
            //        past what we have received. Even though that should never happen?
            let last_index_from_current_leader = input
                .leader_previous_log_entry_index
                .plus(input.new_entries.len() as u64);
            let new_commit_index = cmp::min(input.leader_commit_index, last_index_from_current_leader);
            self.commit_log.ratchet_fwd_commit_index(new_commit_index);
        }

        let output = Ok(AppendEntriesOutput {});

        // > If commitIndex > lastApplied: increment lastApplied, apply
        // > log[lastApplied] to state machine (§5.3)
        if let Err(e) = self.commit_log.try_apply_all_committed_entries() {
            // We've already persisted the log. Applying committed logs is not on critical
            // path. We can wait to retry next time.
            // TODO:5 add logging/metrics. This is a dropped error.
            eprintln!("Error while applying committed entries: {:?}", e);
            // Fall through
        }

        return output;
    }
}
