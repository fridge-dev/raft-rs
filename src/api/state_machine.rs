use bytes::Bytes;

/// StateMachine is the application specific view of applying the replicated logs in sequential
/// consistent order.
pub trait LocalStateMachineApplier {
    /// apply_committed_entry is called only when its guaranteed that the provided entry has been
    /// committed (i.e. replicated to a majority). Because of this guarantee, you will never need
    /// to reverse an entry once you've been asked to apply it, so your impl of applying an entry
    /// need not be reversible. Note: Entry here is synonymous with a state transition.
    ///
    /// The leader will not respond success to the client's request until this method call
    /// completes. Assuming you want to be able to serve strongly consistent reads, that means you
    /// must synchronously and consistently apply the entry before returning. You can optionally
    /// provide an opaque data blob as a result of applying the state transition, and we will return
    /// it to your client. This way, a client can correspond a state machines action and output with
    /// its requested state transition.
    // TODO:2 is `& mut` correct? Might need `&` instead so app can serve reads.
    fn apply_committed_entry(&mut self, entry: Bytes) -> StateMachineOutput;

    // TODO:3 implement snapshotting.
}

pub enum StateMachineOutput {
    Data(Bytes),
    NoData,
}
