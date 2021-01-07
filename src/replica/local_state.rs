use crate::ReplicaId;

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq)]
pub struct Term(u64);

impl Term {
    pub fn new(term: u64) -> Self {
        Term(term)
    }

    pub fn into_inner(self) -> u64 {
        self.0
    }
}

/// PersistentLocalState is used whenever the raft spec requires that something is persisted to a
/// durable store to guarantee safety. Not everything that uses disk has to go through this, only
/// algorithm-correctness-critical ones.
pub trait PersistentLocalState {
    fn store_term_if_increased(&mut self, new_term: Term) -> bool;
    fn store_vote_for_term_if_unvoted(&mut self, expected_term: Term, vote: ReplicaId) -> bool;
    fn current_term(&self) -> Term;
    fn voted_for_current_term(&self) -> (Term, Option<&ReplicaId>);
}

// Currently, this is not persistent. It's just in memory. But I'm focusing on raft algorithm more
// so than integrating with disk correctly.
// TODO:3 Persist local state to disk, not RAM.
pub struct VolatileLocalState {
    current_term: Term,
    voted_for_this_term: Option<ReplicaId>,
}

impl VolatileLocalState {
    pub fn new(/* This will need ClusterId for directory namespace */) -> Self {
        VolatileLocalState {
            current_term: Term::new(0),
            voted_for_this_term: None,
        }
    }
}

// LOL @ that impl signature.
impl PersistentLocalState for VolatileLocalState {
    fn store_term_if_increased(&mut self, new_term: Term) -> bool {
        if new_term <= self.current_term {
            return false;
        } else {
            self.current_term = new_term;
            self.voted_for_this_term = None;
            return true;
        }
    }

    fn store_vote_for_term_if_unvoted(&mut self, expected_term: Term, vote: ReplicaId) -> bool {
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

    fn current_term(&self) -> Term {
        self.current_term
    }

    fn voted_for_current_term(&self) -> (Term, Option<&ReplicaId>) {
        (self.current_term, self.voted_for_this_term.as_ref())
    }
}
