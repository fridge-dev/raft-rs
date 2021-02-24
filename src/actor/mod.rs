use crate::commitlog;
use crate::replica;
use std::error::Error;
use std::fmt::Debug;
use tokio::sync::{mpsc, oneshot};

pub fn create<L, S>(buffer_size: usize, replica: replica::Replica<L, S>) -> (ActorClient, ReplicaActor<L, S>)
where
    L: commitlog::Log<replica::RaftLogEntry>,
    S: replica::PersistentLocalState,
{
    let (tx, rx) = mpsc::channel(buffer_size);
    let client = ActorClient { sender: tx };
    let handler = ReplicaActor { receiver: rx, replica };

    (client, handler)
}

// v1 Design choice: Disk interaction will be synchronous. Future improvement: There should be a
//                   Disk Actor.
//
// v1 Design choice: Raft lib will contain application state machine. Future improvement: We should
//                   just send to a channel, where the app has the receiver and applies to state
//                   machine.
#[derive(Debug)]
enum Event {
    // Leader: Write to disk, locally buffer entry to be replicated later. Also stores callback.
    // Candidate: Reject request.
    // Follower: Redirect.
    WriteToLog(
        replica::WriteToLogInput,
        Callback<replica::WriteToLogOutput, replica::WriteToLogError>,
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
    RequestVoteResultFromPeer(replica::RequestVoteResultFromPeerInput),

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
    AppendEntriesResultFromPeer(replica::AppendEntriesResultFromPeerInput),

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

#[derive(Debug)]
struct Callback<O: Debug, E: Error>(oneshot::Sender<Result<O, E>>);

impl<O: Debug, E: Error> Callback<O, E> {
    pub fn send(self, message: Result<O, E>) {
        let _ = self.0.send(message);
    }
}

pub struct ActorClient {
    // When to use try_send vs send? Do all calls have same criticality?
    sender: mpsc::Sender<Event>,
}

impl ActorClient {
    pub async fn write_to_log(
        &self,
        input: replica::WriteToLogInput,
    ) -> Result<replica::WriteToLogOutput, replica::WriteToLogError> {
        let (tx, rx) = oneshot::channel();
        self.send(Event::WriteToLog(input, Callback(tx))).await;

        rx.await
            .expect("Raft replica event loop actor dropped our channel. WTF!")
    }

    pub async fn request_vote(
        &self,
        input: replica::RequestVoteInput,
    ) -> Result<replica::RequestVoteOutput, replica::RequestVoteError> {
        let (tx, rx) = oneshot::channel();
        self.send(Event::RequestVote(input, Callback(tx))).await;

        rx.await
            .expect("Raft replica event loop actor dropped our channel. WTF!")
    }

    pub async fn request_vote_result_from_peer(&self, input: replica::RequestVoteResultFromPeerInput) {
        self.send(Event::RequestVoteResultFromPeer(input)).await;
    }

    pub async fn append_entries(
        &self,
        input: replica::AppendEntriesInput,
    ) -> Result<replica::AppendEntriesOutput, replica::AppendEntriesError> {
        let (tx, rx) = oneshot::channel();
        self.send(Event::AppendEntries(input, Callback(tx))).await;

        rx.await
            .expect("Raft replica event loop actor dropped our channel. WTF!")
    }

    pub async fn append_entries_result_from_peer(&self, input: replica::AppendEntriesResultFromPeerInput) {
        self.send(Event::AppendEntriesResultFromPeer(input)).await;
    }

    pub async fn leader_timer(&self) {
        self.send(Event::LeaderTimer).await;
    }

    pub async fn follower_timeout(&self) {
        self.send(Event::FollowerTimeout).await;
    }

    async fn send(&self, event: Event) {
        self.sender
            .send(event)
            .await
            .expect("Raft replica event loop actor is dead. WTF!!");
    }
}

/// ReplicaActor is replica logic in actor model.
pub struct ReplicaActor<L, S>
where
    L: commitlog::Log<replica::RaftLogEntry>,
    S: replica::PersistentLocalState,
{
    receiver: mpsc::Receiver<Event>,
    replica: replica::Replica<L, S>,
}

impl<L, S> ReplicaActor<L, S>
where
    L: commitlog::Log<replica::RaftLogEntry>,
    S: replica::PersistentLocalState,
{
    pub async fn run_event_loop(mut self) {
        while let Some(event) = self.receiver.recv().await {
            self.handle_event(event);
        }
    }

    // This must NOT be async. Any long running work must be spawned on another actor
    // and/or come as a callback to this actor.
    fn handle_event(&mut self, event: Event) {
        match event {
            Event::WriteToLog(input, callback) => {
                // Need to pass callback into replica.
                let result = self.replica.write_to_log(input);
                callback.send(result);
            }
            Event::RequestVote(input, callback) => {
                let result = self.replica.handle_request_vote(input);
                callback.send(result);
            }
            // TODO:1 revisit naming for consistency. Base name should have {name}Input, {name}Output, {name}Error, handle_{name}, etc.
            Event::RequestVoteResultFromPeer(input) => {
                self.replica.request_vote_result_from_peer(input);
            }
            Event::AppendEntries(input, callback) => {
                let result = self.replica.handle_append_entries(input);
                callback.send(result);
            }
            Event::AppendEntriesResultFromPeer(input) => {
                self.replica.append_entries_result_from_peer(input);
            }
            Event::LeaderTimer => {
                self.replica.leader_timer();
            }
            Event::FollowerTimeout => {
                self.replica.follower_timeout();
            }
        }
    }
}
