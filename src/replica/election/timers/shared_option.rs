use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub(super) struct SharedOption<T> {
    data: Arc<Mutex<Option<T>>>,
}

impl<T> SharedOption<T> {
    pub(super) fn new() -> Self {
        SharedOption {
            data: Arc::new(Mutex::new(None)),
        }
    }

    pub(super) fn replace(&self, new_data: T) {
        self.data
            .lock()
            .expect("SharedOption.replace() mutex guard poison")
            .replace(new_data);
    }

    pub(super) fn take(&self) -> Option<T> {
        self.data.lock().expect("SharedOption.take() mutex guard poison").take()
    }
}
