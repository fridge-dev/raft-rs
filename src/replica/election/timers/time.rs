use tokio::sync::watch;
use tokio::time::{Duration, Instant};

#[async_trait::async_trait]
pub(crate) trait Clock: Clone {
    fn now(&self) -> Instant;
    async fn sleep_until(&mut self, deadline: Instant);

    async fn sleep(&mut self, duration: Duration) {
        let deadline = self.now() + duration;
        self.sleep_until(deadline).await;
    }
}

#[derive(Copy, Clone)]
pub(crate) struct RealClock;

#[async_trait::async_trait]
impl Clock for RealClock {
    fn now(&self) -> Instant {
        tokio::time::Instant::now()
    }

    async fn sleep_until(&mut self, deadline: Instant) {
        tokio::time::sleep_until(deadline).await;
    }
}

// TODO:3 move this into some base level crate and export mocks as a test-util feature flag.
#[allow(dead_code)]
pub(crate) fn mocked_clock() -> (MockClock, MockClockController) {
    let now = Instant::now();
    let (tx, rx) = watch::channel(now);
    let sleeper = MockClock { current_time: rx };
    let controller = MockClockController {
        current_time: tx,
        time_of_instantiation: now,
    };

    (sleeper, controller)
}

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct MockClock {
    current_time: watch::Receiver<Instant>,
}

#[async_trait::async_trait]
impl Clock for MockClock {
    fn now(&self) -> Instant {
        *self.current_time.borrow()
    }

    async fn sleep_until(&mut self, deadline: Instant) {
        loop {
            if *self.current_time.borrow() >= deadline {
                return;
            }

            self.current_time.changed().await.expect("Controller dropped");
        }
    }
}

#[allow(dead_code)]
pub(crate) struct MockClockController {
    current_time: watch::Sender<Instant>,
    time_of_instantiation: Instant,
}

#[allow(dead_code)]
impl MockClockController {
    pub(crate) fn current_time(&self) -> Instant {
        *self.current_time.borrow()
    }

    pub(crate) fn elapsed_time(&self) -> Duration {
        self.current_time() - self.time_of_instantiation
    }

    /// Advancing by large steps of time can cause surprising behavior in `sleep_until()` usage.
    /// The only promise of mock `sleep_until` is that it will return when `now` is at or past
    /// the `deadline`. For example, if you call...
    ///
    /// - sleep_until(now + 1ms), then
    /// - advance(5 min)
    ///
    /// ...then `sleep_until` will return to its caller when `now` is roughly 5 minutes past the
    /// provided `deadline`. In general, advance the mock clock at much smaller increments than
    /// the granularity at which you wish to observe things. Much like a real clock.
    pub(crate) fn advance(&mut self, duration: Duration) {
        let now = *self.current_time.borrow();
        let new_now = now + duration;
        self.current_time.send(new_now).expect("MockTime dropped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tokio::time::Duration;

    /// Of course we test our test utility! How else will we know it works.
    #[tokio::test]
    async fn mock_clock() {
        let tick_duration = Duration::from_millis(500);
        let (tx, mut rx) = mpsc::unbounded_channel();

        let (mut mock_clock, mut controller) = mocked_clock();
        let test_start_time = controller.current_time();

        // Setup async ticker task
        tokio::spawn(async move {
            let mut next_wake = test_start_time;
            loop {
                next_wake += tick_duration;
                mock_clock.sleep_until(next_wake).await;
                tx.send(()).expect("receiver shouldn't drop");
            }
        });

        // Create half-tick offset just to make it easier to follow and avoid off-by-1.
        controller.advance(tick_duration / 2);

        // Validate initial state
        tokio::time::timeout(tick_duration * 2, rx.recv())
            .await
            .expect_err("Expected timeout");

        // Advance one tick duration
        controller.advance(tick_duration);
        rx.recv().await.unwrap();
        tokio::time::timeout(tick_duration * 2, rx.recv())
            .await
            .expect_err("Expected timeout");

        // Advance multiple ticks at once
        controller.advance(tick_duration * 3);
        rx.recv().await.unwrap();
        rx.recv().await.unwrap();
        rx.recv().await.unwrap();
        tokio::time::timeout(tick_duration * 2, rx.recv())
            .await
            .expect_err("Expected timeout");

        // Assert amount of time that has passed, 4.5 tick durations
        assert_eq!(controller.elapsed_time(), tick_duration * 9 / 2);
    }
}