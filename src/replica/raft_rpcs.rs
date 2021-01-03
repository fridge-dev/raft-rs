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
    candidate_term: u64,
    candidate_id: String,
    candidate_last_log_entry_index: u64,
    candidate_last_log_entry_term: u64,
}

pub struct RequestVoteOutput {
    vote_granted: bool,
}

pub enum RequestVoteError {
    RequestTermOutOfDate(TermOutOfDateInfo),
}

pub struct AppendEntriesInput {
    leader_term: u64,
    leader_id: String,
    leader_previous_log_entry_index: u64,
    leader_previous_log_entry_term: u64,
    leader_commit_index: u64,
}

pub struct AppendEntriesOutput {
    // Nothing
}

pub enum AppendEntriesError {
    RequestTermOutOfDate(TermOutOfDateInfo),
    FollowerMissingPreviousLogEntry,
}

pub struct TermOutOfDateInfo {
    current_term: u64,
}
