use crate::replica::peers::ReplicaId;
use std::fmt;
use std::sync::Arc;

#[derive(Copy, Clone, PartialOrd, PartialEq)]
pub(crate) struct Term(u64);

impl Term {
    pub(crate) fn new(term: u64) -> Self {
        Term(term)
    }

    pub(crate) fn as_u64(&self) -> u64 {
        self.0
    }

    pub(crate) fn incr(&mut self) {
        self.0 += 1;
    }
}

impl fmt::Debug for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// PersistentLocalState is used whenever the raft spec requires that something is persisted to a
/// durable store to guarantee safety. Not everything that uses disk has to go through this, only
/// algorithm-correctness-critical ones.
///
/// Store methods should be implemented atomically via a CAS like operation. Similar to most CAS
/// method signatures, the CAS store methods will return true if we have mutated state.
pub(crate) trait PersistentLocalState {
    /// Set current term to `new_term` atomically, iff it is larger than current term.
    ///
    /// CAS: Return true if we successfully mutated state.
    fn store_term_if_increased(&mut self, new_term: Term) -> bool;

    /// Store our vote for the latest term iff the latest term (internal state) is the same term as
    /// the one provided, and we have not stored a vote for the latest term.
    ///
    /// CAS: Return true if we successfully mutated state.
    fn store_vote_for_term_if_unvoted(&mut self, expected_current_term: Term, vote: ReplicaId) -> bool;

    /// Return the new term. Used when transitioning to candidate.
    fn increment_term_and_vote_for_self(&mut self) -> Term;

    fn current_term(&self) -> Term;
    fn voted_for_current_term(&self) -> (Term, Option<Arc<ReplicaId>>);
}

// Currently, this is not persistent. It's just in memory. But I'm focusing on raft algorithm more
// so than integrating with disk correctly.
// TODO:3 Persist local state to disk, not RAM.
pub(super) struct VolatileLocalState {
    current_term: Term,
    voted_for_this_term: Option<Arc<ReplicaId>>,
    my_replica_id: Arc<ReplicaId>,
}

impl VolatileLocalState {
    pub(super) fn new(my_replica_id: ReplicaId) -> Self {
        VolatileLocalState {
            current_term: Term::new(0),
            voted_for_this_term: None,
            my_replica_id: Arc::new(my_replica_id),
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
                self.voted_for_this_term.replace(Arc::new(vote));
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    fn increment_term_and_vote_for_self(&mut self) -> Term {
        self.current_term.incr();
        self.voted_for_this_term.replace(self.my_replica_id.clone());

        self.current_term
    }

    fn current_term(&self) -> Term {
        self.current_term
    }

    fn voted_for_current_term(&self) -> (Term, Option<Arc<ReplicaId>>) {
        (self.current_term, self.voted_for_this_term.clone())
    }
}
