use crate::commitlog::Index;
use crate::replica::replica::Term;

// We need this internally.
pub trait RaftRpcHandler {
    fn handle_request_vote(
        &mut self,
        input: RequestVoteInput,
    ) -> Result<RequestVoteOutput, RequestVoteError>;
    fn handle_append_entries(
        &mut self,
        input: AppendEntriesInput,
    ) -> Result<AppendEntriesOutput, AppendEntriesError>;
}

pub struct RequestVoteInput {
    candidate_term: Term,
    candidate_id: String,
    candidate_last_log_entry_index: Index,
    candidate_last_log_entry_term: Term,
}

pub struct RequestVoteOutput {
    vote_granted: bool,
}

pub enum RequestVoteError {
    RequestTermOutOfDate(TermOutOfDateInfo),
}

pub struct AppendEntriesInput {
    leader_term: Term,
    leader_id: String,
    leader_previous_log_entry_index: Index,
    leader_previous_log_entry_term: Term,
    leader_commit_index: Index,
}

pub struct AppendEntriesOutput {
    // Nothing
}

pub enum AppendEntriesError {
    RequestTermOutOfDate(TermOutOfDateInfo),
    FollowerMissingPreviousLogEntry,
}

pub struct TermOutOfDateInfo {
    current_term: Term,
}
