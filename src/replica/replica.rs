use crate::commitlog::{Index, Log};
use crate::replica::commit_log::{CommitLog, RaftLogEntry};
use crate::replica::election::{CurrentLeader, ElectionState};
use crate::replica::local_state::{PersistentLocalState, Term};
use crate::replica::peers::MemberInfo;
use crate::replica::raft_rpcs::{
    AppendEntriesError, AppendEntriesInput, AppendEntriesOutput, LeaderLogEntry, RaftClientApi,
    RaftRpcHandler, RequestVoteError, RequestVoteInput, RequestVoteOutput, TermOutOfDateInfo,
    WriteToLogError, WriteToLogInput, WriteToLogOutput,
};
use crate::replica::state_machine::{StateMachine, StateMachineOutput};
use crate::replica::ReplicaId;
use bytes::Bytes;
use std::cmp;
use std::collections::HashMap;
use std::hash::Hash;

pub struct ReplicaConfig<L, S, M>
where
    L: Log<RaftLogEntry>,
    S: PersistentLocalState,
    M: StateMachine,
{
    pub my_replica_id: ReplicaId,
    pub cluster_members: Vec<MemberInfo>,
    pub log: L,
    pub local_state: S,
    pub state_machine: M,
}

pub struct RaftReplica<L, S, M>
where
    L: Log<RaftLogEntry>,
    S: PersistentLocalState,
    M: StateMachine,
{
    my_replica_id: ReplicaId,
    cluster_members: HashMap<ReplicaId, MemberInfo>,
    local_state: S,
    election_state: ElectionState,
    commit_log: CommitLog<L>,
    state_machine: M,
}

impl<L, S, M> RaftReplica<L, S, M>
where
    L: Log<RaftLogEntry>,
    S: PersistentLocalState,
    M: StateMachine,
{
    pub fn new(config: ReplicaConfig<L, S, M>) -> Self {
        let commit_log = CommitLog::new(config.log);
        let cluster_members = map_with_unique_index(config.cluster_members, |m| m.id.clone())
            .expect("Cluster members have duplicate ReplicaId.");
        RaftReplica {
            my_replica_id: config.my_replica_id,
            cluster_members,
            local_state: config.local_state,
            election_state: ElectionState::new_follower(),
            commit_log,
            state_machine: config.state_machine,
        }
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
        let (my_last_entry_term, my_last_entry_index) = self.commit_log.latest_entry();
        if candidate_last_entry_term > my_last_entry_term {
            return true;
        } else if candidate_last_entry_term < my_last_entry_term {
            return false;
        }

        candidate_last_entry_index > my_last_entry_index
    }
}

impl<L, S, M> RaftRpcHandler for RaftReplica<L, S, M>
where
    L: Log<RaftLogEntry>,
    S: PersistentLocalState,
    M: StateMachine,
{
    fn handle_request_vote(
        &mut self,
        input: RequestVoteInput,
    ) -> Result<RequestVoteOutput, RequestVoteError> {
        // Ensure candidate is known member.
        if !self.cluster_members.contains_key(&input.candidate_id) {
            return Err(RequestVoteError::CandidateNotInCluster);
        }

        // 1. Reply false if term < currentTerm (§5.1)
        let current_term = self.local_state.current_term();
        if input.candidate_term < current_term {
            return Err(RequestVoteError::RequestTermOutOfDate(TermOutOfDateInfo {
                current_term,
            }));
        }

        // > If RPC request or response contains term T > currentTerm:
        // > set currentTerm = T, convert to follower (§5.1)
        let increased = self
            .local_state
            .store_term_if_increased(input.candidate_term);
        if increased {
            self.election_state.transition_to_follower();
        }

        // 2. If votedFor is null or candidateId, and candidate’s log is at
        // least as up-to-date as receiver’s log, grant vote (§5.2, §5.4).

        // If votedFor is null or candidateId, and...
        let (_, opt_voted_for) = self.local_state.voted_for_current_term(); // TODO:1 replace method
        let can_vote_for_candidate = match opt_voted_for {
            None => true,
            Some(voted_for) => *voted_for == input.candidate_id,
        };
        let already_voted_for_someone_else = !can_vote_for_candidate;
        if already_voted_for_someone_else {
            return Ok(RequestVoteOutput {
                vote_granted: false,
            });
        }

        // ...and candidate’s log is at least as up-to-date as receiver’s log...
        if !self.is_candidate_more_up_to_date_than_me(
            input.candidate_last_log_entry_term,
            input.candidate_last_log_entry_index,
        ) {
            return Ok(RequestVoteOutput {
                vote_granted: false,
            });
        }

        // ...grant vote
        // TODO:2 Idk why I initially wrote "_if_unvoted" here. We've already validated above
        //        that we haven't voted. I guess because we can't easily guarantee atomic or
        //        exclusive write to disk? We'll see if this is still needed.
        //        Edit: I guess it's possible that this is retry from our already-voted-for
        //        candidate, and we should handle it idempotent-ly.
        let _ = self
            .local_state
            .store_vote_for_term_if_unvoted(input.candidate_term, input.candidate_id);

        // If "unvoted" wasn't true, it means this is a retry, and it's safe to return true.
        // If term changed due to race condition with disk, then frick, but everyone else will
        // reject this candidate as out of date eventually, so this should be fine.
        Ok(RequestVoteOutput { vote_granted: true })
    }

    fn handle_append_entries(
        &mut self,
        input: AppendEntriesInput,
    ) -> Result<AppendEntriesOutput, AppendEntriesError> {
        // 0. Ensure candidate is known member.
        if !self.cluster_members.contains_key(&input.leader_id) {
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
            self.election_state.transition_to_follower();
        }

        // 2. Reply false if [my] log doesn't contain an entry at [leader's]
        // prevLogIndex whose term matches [leader's] prevLogTerm (§5.3)
        let me_missing_entry = match self.commit_log.read(input.leader_previous_log_entry_index) {
            Ok(Some(my_previous_log_entry)) => {
                Ok(my_previous_log_entry.term != input.leader_previous_log_entry_term)
            }
            Ok(None) => Ok(true),
            Err(e) => Err(AppendEntriesError::ServerIoError(e)),
        }?;
        if me_missing_entry {
            // 3. If [my] existing entry conflicts with [leader's] (same index
            // but different terms), delete [my] existing entry and all that
            // follow it (§5.3)
            self.commit_log.rewind_single_entry();
            return Err(AppendEntriesError::ServerMissingPreviousLogEntry);
        }

        // 4. Append any new entries not already in the log
        let opt_index_of_last_new_entry = input.entries.last().map(|last_entry| last_entry.index);
        self.commit_log
            .append(input.entries)
            .map_err(|e| AppendEntriesError::ServerIoError(e))?;

        // 5. If leaderCommit > commitIndex, set commitIndex =
        // min(leaderCommit, index of last new entry)
        if input.leader_commit_index > self.commit_log.commit_index {
            // TODO:1 wtf to do if entries was empty?? Paper doesn't state this. Formal spec probably does though.
            //        For now, assuming it's safe to update. Eventually, read spec to confirm.
            let new_commit_index = match opt_index_of_last_new_entry {
                Some(index_of_last_new_entry) => {
                    cmp::min(input.leader_commit_index, index_of_last_new_entry)
                }
                None => input.leader_commit_index,
            };

            // Sanity check because I'm uncertain. Re-read paper/spec.
            let (_, my_last_entry_index) = self.commit_log.latest_entry();
            assert!(
                new_commit_index <= my_last_entry_index,
                "Raft paper has failed me."
            );

            self.commit_log.commit_index = new_commit_index;
        }

        let output = Ok(AppendEntriesOutput {});

        // > If commitIndex > lastApplied: increment lastApplied, apply
        // > log[lastApplied] to state machine (§5.3)
        while self.commit_log.last_applied_index < self.commit_log.commit_index {
            let mut next_index = self.commit_log.last_applied_index;
            next_index.incr(1);

            // TODO:2 implement batch/streamed read.
            let entry = match self.commit_log.read(next_index) {
                Ok(Some(entry)) => entry,
                Ok(None) => {
                    // TODO:2 validate and/or gracefully handle.
                    panic!("This should never happen #5230185123");
                }
                Err(_) => {
                    // We've already persisted the log. Applying committed logs is not on critical
                    // path. We can wait to retry next time.
                    // TODO:5 add logging/metrics. This is a dropped error.
                    return output;
                }
            };

            let _ = self
                .state_machine
                .apply_committed_entry(Bytes::from(entry.data));
            self.commit_log.last_applied_index = next_index;
        }

        return output;
    }
}

impl<L, S, M> RaftClientApi for RaftReplica<L, S, M>
where
    L: Log<RaftLogEntry>,
    S: PersistentLocalState,
    M: StateMachine,
{
    fn write_to_log(
        &mut self,
        input: WriteToLogInput,
    ) -> Result<WriteToLogOutput, WriteToLogError> {
        // Leader check
        match self.election_state.current_leader() {
            CurrentLeader::Me => { /* carry on */ }
            CurrentLeader::Other(leader_id) => match self.cluster_members.get(&leader_id) {
                Some(leader) => {
                    return Err(WriteToLogError::FollowerRedirect {
                        leader_id,
                        leader_ip: leader.ip,
                    });
                }
                None => {
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
        let index = self.commit_log.next_index();
        // TODO:1 leader needs own API
        let new_entry = LeaderLogEntry {
            term,
            index,
            data: input.data.clone(),
        };
        self.commit_log
            .append(vec![new_entry])
            .map_err(|e| WriteToLogError::LocalIoError(e))?;

        // TODO:2 (after gRPC setup and threading/timers) replicate entries
        let applier_outcome = match self
            .state_machine
            .apply_committed_entry(Bytes::from(input.data))
        {
            StateMachineOutput::Data(outcome) => outcome,
            StateMachineOutput::NoData => Bytes::new(),
        };

        Ok(WriteToLogOutput {
            // TODO:2 decide on byte repr of Vec<u8> vs Bytes. This is wasteful.
            applier_outcome: applier_outcome.to_vec(),
        })
    }
}

/// Returns a HashMap that is guaranteed to have uniquely indexed all of the values. If duplicate is
/// present, the key for the duplicate is returned as an Err.
fn map_with_unique_index<K, V, F>(values: Vec<V>, key_for_value: F) -> Result<HashMap<K, V>, K>
where
    K: Hash + Eq,
    F: Fn(&V) -> K,
{
    let mut map = HashMap::with_capacity(values.len());

    for v in values {
        if let Some(duplicate) = map.insert(key_for_value(&v), v) {
            return Err(key_for_value(&duplicate));
        }
    }

    Ok(map)
}
