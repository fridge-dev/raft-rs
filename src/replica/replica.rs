use crate::commitlog::{Index, Log};
use crate::replica::commit_log::{RaftLogEntry, CommitLog};
use crate::replica::election::ElectionState;
use crate::replica::local_state::{PersistentLocalState, Term};
use crate::replica::peers::MemberInfo;
use crate::replica::raft_rpcs::{
    AppendEntriesError, AppendEntriesInput, AppendEntriesOutput, RaftRpcHandler, RequestVoteError,
    RequestVoteInput, RequestVoteOutput, TermOutOfDateInfo,
};
use crate::replica::state_machine::StateMachine;
use crate::replica::ReplicaId;
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
    fn is_candidate_more_up_to_date_than_me(&self, candidate_last_entry_term: Term, candidate_last_entry_index: Index) -> bool {
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

        // > If votedFor is null or candidateId, and candidate’s log is at
        // > least as up-to-date as receiver’s log, grant vote (§5.2, §5.4).

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
        if !self.is_candidate_more_up_to_date_than_me(input.candidate_last_log_entry_term, input.candidate_last_log_entry_index) {
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
        _input: AppendEntriesInput,
    ) -> Result<AppendEntriesOutput, AppendEntriesError> {
        unimplemented!()
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
