/// PersistentLocalState is used whenever the raft spec requires that something is persisted to a
/// durable store to guarantee safety. Not everything that uses disk has to go through this, only
/// algorithm-correctness-critical ones.
//
// Currently, this is not persistent. It's just in memory.
// TODO: Persist local state to disk, not RAM.
pub struct PersistentLocalState {
    current_term: u64,
    voted_for_this_term: Option<String>,
}

impl PersistentLocalState {
    pub fn new(/* This will need ClusterId for directory namespace */) -> Self {
        PersistentLocalState {
            current_term: 0,
            voted_for_this_term: None,
        }
    }

    pub fn store_term_if_increased(&mut self, new_term: u64) -> bool {
        if new_term <= self.current_term {
            return false;
        } else {
            self.current_term = new_term;
            self.voted_for_this_term = None;
            return true;
        }
    }

    pub fn store_vote_for_term_if_unvoted(&mut self, expected_term: u64, vote: String) -> bool {
        if expected_term == self.current_term {
            if self.voted_for_this_term.is_none() {
                self.voted_for_this_term.replace(vote);
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn current_term(&self) -> u64 {
        self.current_term
    }

    pub fn voted_for_current_term(&self) -> (u64, Option<&String>) {
        (self.current_term, self.voted_for_this_term.as_ref())
    }
}