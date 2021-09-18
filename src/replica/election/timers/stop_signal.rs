use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub(super) struct Stopper {
    stop_signal: Arc<AtomicBool>,
}

pub(super) struct StopCheck {
    stop_signal: Arc<AtomicBool>,
}

impl Drop for Stopper {
    fn drop(&mut self) {
        self.stop_signal.store(true, Ordering::Release);
    }
}

impl StopCheck {
    pub(super) fn should_stop(&self) -> bool {
        self.stop_signal.load(Ordering::Acquire)
    }
}

pub(super) fn new() -> (Stopper, StopCheck) {
    let stop_signal = Arc::new(AtomicBool::new(false));

    let stopper = Stopper {
        stop_signal: stop_signal.clone(),
    };
    let stop_check = StopCheck { stop_signal };

    (stopper, stop_check)
}
