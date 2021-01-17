use crate::api::client::{RaftClientApi, WriteToLogError, WriteToLogInput, WriteToLogOutput};
use crate::api::state_machine::LocalStateMachineApplier;
use crate::commitlog::Log;
use crate::replica::{PersistentLocalState, RaftLogEntry, Replica};

pub struct PlaceholderImpl<L, S, M>
where
    L: Log<RaftLogEntry>,
    S: PersistentLocalState,
    M: LocalStateMachineApplier,
{
    pub replica: Replica<L, S, M>,
}

impl<L, S, M> RaftClientApi for PlaceholderImpl<L, S, M>
where
    L: Log<RaftLogEntry>,
    S: PersistentLocalState,
    M: LocalStateMachineApplier,
{
    fn write_to_log(&mut self, input: WriteToLogInput) -> Result<WriteToLogOutput, WriteToLogError> {
        self.replica.write_to_log(input)
    }
}
