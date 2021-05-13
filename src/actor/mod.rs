use crate::commitlog;
use crate::replica;
use std::error::Error;
use std::fmt;
use std::fmt::{Debug, Formatter};
use tokio::sync::{mpsc, oneshot};

// v1 Design choice: Disk interaction will be synchronous. Future improvement: There should be a
//                   Disk Actor.
#[derive(Debug)]
pub enum Event {
    // Leader: Write to disk, locally buffer entry to be replicated later. Also stores callback.
    // Candidate: Reject request.
    // Follower: Redirect.
    EnqueueForReplication(
        replica::EnqueueForReplicationInput,
        Callback<replica::EnqueueForReplicationOutput, replica::EnqueueForReplicationError>,
    ),

    // Leader: Grant vote if applicable (includes write to disk). Transition to follower.
    // Candidate: Grant vote if applicable (includes write to disk). Transition to follower.
    // Follower: Grant vote if applicable (includes write to disk).
    RequestVote(
        replica::RequestVoteInput,
        Callback<replica::RequestVoteOutput, replica::RequestVoteError>,
    ),

    // Leader: discard
    // Candidate: Update local state. Transition to leader if quorum vote.
    // Follower: discard
    RequestVoteReplyFromPeer(replica::RequestVoteReplyFromPeer),

    // Leader: Transition to follower if applicable. Clean up log. Respond to request.
    // Candidate: Transition to follower if applicable. Clean up log. Respond to request.
    // Follower: Write to disk then respond. Reset timeout.
    AppendEntries(
        replica::AppendEntriesInput,
        Callback<replica::AppendEntriesOutput, replica::AppendEntriesError>,
    ),

    // Leader: Update local state tracking each entry's replication progress. If committed, apply to state machine and send response to WTL client.
    // Candidate: discard
    // Follower: discard
    AppendEntriesReplyFromPeer(replica::AppendEntriesReplyFromPeer),

    // Thought: Separate channel for timer/heartbeat events?
    // Thought: Combine heartbeat timer into single timer? Take different action based on state?

    // Leader: Call AppendEntries for peer with all local un-replicated entries, or send empty heartbeat.
    // Candidate: NOT POSSIBLE - discard
    // Follower: NOT POSSIBLE - discard
    LeaderTimer(replica::LeaderTimerTick),

    // Leader: NOT POSSIBLE - discard
    // Candidate: Transition to candidate. Trigger new election.
    // Follower: Transition to candidate. Trigger new election.
    FollowerTimeout, /* Payload? */
}

pub struct Callback<O: Debug, E: Error>(oneshot::Sender<Result<O, E>>);

impl<O: Debug, E: Error> Debug for Callback<O, E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Callback").finish()
    }
}

impl<O: Debug, E: Error> Callback<O, E> {
    pub fn send(self, message: Result<O, E>) {
        let _ = self.0.send(message);
    }
}

#[derive(Clone)]
pub struct ActorClient {
    // When to use try_send vs send? Do all calls have same criticality?
    sender: mpsc::Sender<Event>,
}

impl ActorClient {
    pub fn new(sender: mpsc::Sender<Event>) -> Self {
        ActorClient { sender }
    }

    pub async fn enqueue_for_replication(
        &self,
        input: replica::EnqueueForReplicationInput,
    ) -> Result<replica::EnqueueForReplicationOutput, replica::EnqueueForReplicationError> {
        let (tx, rx) = oneshot::channel();
        self.send_to_actor(Event::EnqueueForReplication(input, Callback(tx)))
            .await
            .expect("Raft replica event loop actor is dead. WTF!!");

        rx.await
            // TODO:1 remove possibility of panic for clean replica termination
            .expect("Raft replica event loop actor dropped our channel. WTF!")
    }

    pub async fn request_vote(
        &self,
        input: replica::RequestVoteInput,
    ) -> Result<replica::RequestVoteOutput, replica::RequestVoteError> {
        let (tx, rx) = oneshot::channel();
        self.send_to_actor(Event::RequestVote(input, Callback(tx)))
            .await
            .map_err(|_| replica::RequestVoteError::ActorDead)?;

        rx.await.map_err(|_| replica::RequestVoteError::ActorDead)?
    }

    pub async fn notify_request_vote_reply_from_peer(&self, reply: replica::RequestVoteReplyFromPeer) {
        self.send_to_actor(Event::RequestVoteReplyFromPeer(reply))
            .await
            .expect("Raft replica event loop actor is dead. WTF!!");
    }

    pub async fn append_entries(
        &self,
        input: replica::AppendEntriesInput,
    ) -> Result<replica::AppendEntriesOutput, replica::AppendEntriesError> {
        let (tx, rx) = oneshot::channel();
        self.send_to_actor(Event::AppendEntries(input, Callback(tx)))
            .await
            .map_err(|_| replica::AppendEntriesError::ActorDead)?;

        rx.await.map_err(|_| replica::AppendEntriesError::ActorDead)?
    }

    pub async fn notify_append_entries_reply_from_peer(&self, reply: replica::AppendEntriesReplyFromPeer) {
        self.send_to_actor(Event::AppendEntriesReplyFromPeer(reply))
            .await
            .expect("Raft replica event loop actor is dead. WTF!!");
    }

    pub async fn leader_timer(&self, input: replica::LeaderTimerTick) {
        self.send_to_actor(Event::LeaderTimer(input))
            .await
            .expect("Raft replica event loop actor is dead. WTF!!");
    }

    pub async fn follower_timeout(&self) {
        self.send_to_actor(Event::FollowerTimeout)
            .await
            .expect("Raft replica event loop actor is dead. WTF!!");
    }

    async fn send_to_actor(&self, event: Event) -> Result<(), ()> {
        self.sender.send(event).await.map_err(|_| ())
    }
}

/// ReplicaActor is replica logic in actor model.
pub struct ReplicaActor<L, S>
where
    L: commitlog::Log<replica::RaftLogEntry>,
    S: replica::PersistentLocalState,
{
    logger: slog::Logger,
    receiver: mpsc::Receiver<Event>,
    replica: replica::Replica<L, S>,
}

impl<L, S> ReplicaActor<L, S>
where
    L: commitlog::Log<replica::RaftLogEntry> + 'static,
    S: replica::PersistentLocalState + 'static,
{
    pub fn new(logger: slog::Logger, receiver: mpsc::Receiver<Event>, replica: replica::Replica<L, S>) -> Self {
        ReplicaActor {
            logger,
            receiver,
            replica,
        }
    }

    pub async fn run_event_loop(mut self) {
        while let Some(event) = self.receiver.recv().await {
            slog::trace!(self.logger, "Received: {:?}", event);
            self.handle_event(event);
        }
    }

    // This must NOT be async. Any long running work must be spawned on another actor
    // and/or come as a callback to this actor.
    fn handle_event(&mut self, event: Event) {
        match event {
            Event::EnqueueForReplication(input, callback) => {
                // Need to pass callback into replica.
                let result = self.replica.handle_enqueue_for_replication(input);
                callback.send(result);
            }
            Event::RequestVote(input, callback) => {
                let result = self.replica.server_handle_request_vote(input);
                callback.send(result);
            }
            Event::RequestVoteReplyFromPeer(reply) => {
                self.replica.handle_request_vote_reply_from_peer(reply);
            }
            Event::AppendEntries(input, callback) => {
                let result = self.replica.server_handle_append_entries(input);
                callback.send(result);
            }
            Event::AppendEntriesReplyFromPeer(reply) => {
                self.replica.handle_append_entries_reply_from_peer(reply);
            }
            Event::LeaderTimer(input) => {
                self.replica.handler_leader_timer(input);
            }
            Event::FollowerTimeout => {
                self.replica.handle_follower_timeout();
            }
        }
    }
}
