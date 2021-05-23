use crate::replica::ElectionStateChangeListener;
use crate::replica::ElectionStateSnapshot;

// This is a really lazy event bus style just to expose *any* API to the consumer. I will probably
// have to re-write this at some point to be more easily usable and expose APIs for properly filtered
// set of topics, but we don't need that right now.

/// An event that happened, as observed by the local raft replica.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Event {
    /// An event of leader election or timeout. Consuming this event type is subtle. It doesn't queue
    /// intermediate events. If there are multiple events between when application awaits the next event,
    /// those events will be clobbered into only the most recent event.
    Election(ElectionEvent),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ElectionEvent {
    Leader,
    Candidate,
    Follower(FollowerEventData),
    FollowerNoLeader,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FollowerEventData {
    pub leader_replica_id: String,
}

pub struct EventListener {
    election_state_change_listener: ElectionStateChangeListener,
}

impl EventListener {
    pub(crate) fn new(election_state_change_listener: ElectionStateChangeListener) -> Self {
        EventListener {
            election_state_change_listener,
        }
    }

    /// `next_event()` returns the next event that this local raft replica observes.
    pub async fn next_event(&mut self) -> Option<Event> {
        self
            .election_state_change_listener
            .next()
            .await
            .map(|election_state| Event::Election(ElectionEvent::from(election_state)))
    }
}

// ------- Conversions --------

impl From<ElectionStateSnapshot> for ElectionEvent {
    fn from(election_state: ElectionStateSnapshot) -> Self {
        match election_state {
            ElectionStateSnapshot::Leader => ElectionEvent::Leader,
            ElectionStateSnapshot::Candidate => ElectionEvent::Candidate,
            ElectionStateSnapshot::Follower(leader_id) => ElectionEvent::Follower(FollowerEventData {
                leader_replica_id: leader_id.into_inner(),
            }),
            ElectionStateSnapshot::FollowerNoLeader => ElectionEvent::FollowerNoLeader,
        }
    }
}
