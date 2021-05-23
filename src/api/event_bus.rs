use crate::api::LeaderInfo;
use crate::replica;

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
    Follower(LeaderInfo),
    FollowerNoLeader,
}

pub struct EventListener {
    election_state_change_listener: replica::ElectionStateChangeListener,
}

impl EventListener {
    pub(crate) fn new(election_state_change_listener: replica::ElectionStateChangeListener) -> Self {
        EventListener {
            election_state_change_listener,
        }
    }

    /// `next_event()` returns the next event that this local raft replica observes.
    pub async fn next_event(&mut self) -> Option<Event> {
        self.election_state_change_listener
            .next()
            .await
            .map(|election_state| Event::Election(ElectionEvent::from(election_state)))
    }
}

// ------- Conversions --------

impl From<replica::ElectionStateSnapshot> for ElectionEvent {
    fn from(election_state: replica::ElectionStateSnapshot) -> Self {
        match election_state {
            replica::ElectionStateSnapshot::Leader => ElectionEvent::Leader,
            replica::ElectionStateSnapshot::Candidate => ElectionEvent::Candidate,
            replica::ElectionStateSnapshot::Follower(leader) => ElectionEvent::Follower(LeaderInfo::from(leader)),
            replica::ElectionStateSnapshot::FollowerNoLeader => ElectionEvent::FollowerNoLeader,
        }
    }
}
