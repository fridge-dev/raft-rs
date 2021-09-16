use crate::commitlog::{Entry, Index, Log};
use std::marker::PhantomData;
use std::time::Duration;
use std::{io, thread};

// I am focusing on learning raft's consensus algorithm, not strictly about exactly how to
// persist the data durably. I will theoretically model it here.
//
// To improve on this, see storage.rs.
pub(crate) struct InMemoryLog<E: Entry> {
    // We don't *need* to convert these to bytes. We could just hold the original entry in memory,
    // but we want to exercise the conversion logic.
    log: Vec<Vec<u8>>,
    info_log: slog::Logger,
    _pd: PhantomData<E>,
}

impl<E: Entry> InMemoryLog<E> {
    pub(crate) fn create(info_log: slog::Logger) -> Result<Self, io::Error> {
        Ok(InMemoryLog {
            log: vec![],
            info_log,
            _pd: PhantomData::default(),
        })
    }

    fn vec_index(index: Index) -> usize {
        // Log API states that Index starts from 1.
        (index.as_u64() - 1) as usize
    }
}

impl<E: Entry> Log<E> for InMemoryLog<E> {
    // returns the log entry index that was just used to append the entry
    fn append(&mut self, entry: E) -> Result<Index, io::Error> {
        let entry = entry.into();
        slog::info!(self.info_log, "{:?}", entry);
        LogAction::Write.simulate_disk_latency();
        self.log.push(entry.into());

        Ok(Index::new_usize(self.log.len()))
    }

    fn read(&self, index: Index) -> Result<Option<E>, io::Error> {
        let vec_index = Self::vec_index(index);
        LogAction::Read.simulate_disk_latency();
        let opt_entry = self.log.get(vec_index).cloned().map(|b| E::from(b));

        Ok(opt_entry)
    }

    fn truncate(&mut self, index: Index) {
        let vec_index = Self::vec_index(index);
        LogAction::Write.simulate_disk_latency();
        self.log.truncate(vec_index)
    }

    fn next_index(&self) -> Index {
        Index::new_usize(self.log.len() + 1)
    }
}

enum LogAction {
    Read,
    Write,
}

impl LogAction {
    fn simulate_disk_latency(&self) {
        let latency_ms = match self {
            LogAction::Read => 1,
            LogAction::Write => 2,
        };
        thread::sleep(Duration::from_millis(latency_ms));
    }
}
