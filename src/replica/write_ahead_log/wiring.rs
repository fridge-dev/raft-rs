use crate::replica::{CommitStream, WriteAheadLogEntry};
use crate::commitlog;
use crate::replica::write_ahead_log::{WriteAheadLog, commit_stream};

pub(in super::super) fn wired<L>(logger: slog::Logger, log: L) -> (WriteAheadLog<L>, CommitStream)
    where
        L: commitlog::Log<WriteAheadLogEntry>,
{
    let (publisher, stream) = commit_stream::new();

    let wal = WriteAheadLog::new(logger, log, publisher);

    (wal, stream)
}
