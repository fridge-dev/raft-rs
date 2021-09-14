use crate::api::commit_stream::RaftCommitStream;
use crate::api::event_bus::RaftEventListener;
use crate::api::replicated_log::ReplicatedLog;

/// RaftClient is the conglomeration of all of the client facing components.
pub struct RaftClient {
    pub replicated_log: ReplicatedLog,
    pub commit_stream: RaftCommitStream,
    pub event_listener: RaftEventListener,
}

impl RaftClient {
    pub fn destruct(self) -> (ReplicatedLog, RaftCommitStream, RaftEventListener) {
        (self.replicated_log, self.commit_stream, self.event_listener)
    }
}
