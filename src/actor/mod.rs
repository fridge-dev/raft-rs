use crate::commitlog;
use crate::replica;
use prost::alloc::fmt::Formatter;
use std::error::Error;
use std::fmt;
use std::fmt::Debug;
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

    // Leader: Call AppendEntries on all peers with all local un-replicated entries. Initialize local state tracking each entry's replication progress.
    // Candidate: NOT POSSIBLE - discard
    // Follower: NOT POSSIBLE - discard
    LeaderTimer, /* Payload? */

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
            .await;

        rx.await
            .expect("Raft replica event loop actor dropped our channel. WTF!")
    }

    pub async fn request_vote(
        &self,
        input: replica::RequestVoteInput,
    ) -> Result<replica::RequestVoteOutput, replica::RequestVoteError> {
        let (tx, rx) = oneshot::channel();
        self.send_to_actor(Event::RequestVote(input, Callback(tx))).await;

        rx.await
            .expect("Raft replica event loop actor dropped our channel. WTF!")
    }

    pub async fn notify_request_vote_reply_from_peer(&self, reply: replica::RequestVoteReplyFromPeer) {
        self.send_to_actor(Event::RequestVoteReplyFromPeer(reply)).await;
    }

    pub async fn append_entries(
        &self,
        input: replica::AppendEntriesInput,
    ) -> Result<replica::AppendEntriesOutput, replica::AppendEntriesError> {
        let (tx, rx) = oneshot::channel();
        self.send_to_actor(Event::AppendEntries(input, Callback(tx))).await;

        rx.await
            .expect("Raft replica event loop actor dropped our channel. WTF!")
    }

    pub async fn notify_append_entries_reply_from_peer(&self, reply: replica::AppendEntriesReplyFromPeer) {
        self.send_to_actor(Event::AppendEntriesReplyFromPeer(reply)).await;
    }

    pub async fn leader_timer(&self) {
        self.send_to_actor(Event::LeaderTimer).await;
    }

    pub async fn follower_timeout(&self) {
        self.send_to_actor(Event::FollowerTimeout).await;
    }

    async fn send_to_actor(&self, event: Event) {
        self.sender
            .send(event)
            .await
            .expect("Raft replica event loop actor is dead. WTF!!");
    }
}

/// ReplicaActor is replica logic in actor model.
pub struct ReplicaActor<L, S>
where
    L: commitlog::Log<replica::RaftCommitLogEntry>,
    S: replica::PersistentLocalState,
{
    logger: slog::Logger,
    receiver: mpsc::Receiver<Event>,
    replica: replica::Replica<L, S>,
}

impl<L, S> ReplicaActor<L, S>
where
    L: commitlog::Log<replica::RaftCommitLogEntry> + 'static,
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
                let result = self.replica.server_request_vote(input);
                callback.send(result);
            }
            Event::RequestVoteReplyFromPeer(reply) => {
                self.replica.handle_request_vote_reply_from_peer(reply);
            }
            Event::AppendEntries(input, callback) => {
                let result = self.replica.handle_append_entries(input);
                callback.send(result);
            }
            Event::AppendEntriesReplyFromPeer(reply) => {
                self.replica.handle_append_entries_reply_from_peer(reply);
            }
            Event::LeaderTimer => {
                self.replica.handle_leader_timer();
            }
            Event::FollowerTimeout => {
                self.replica.handle_follower_timeout();
            }
        }
    }
}
