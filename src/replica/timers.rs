pub use follower::FollowerTimerHandle;
pub use leader::LeaderTimerHandle;

mod leader {
    use crate::replica::timers::shared_option::SharedOption;
    use crate::replica::timers::time::{Clock, RealClock};
    use crate::{actor, replica};
    use std::sync::{Arc, Weak};
    use tokio::time::{Duration, Instant};

    pub struct LeaderTimerHandle<C: Clock = RealClock> {
        shared: Arc<Shared<C>>,
    }

    struct Shared<C: Clock> {
        heartbeat_duration: Duration,
        next_heartbeat_time: SharedOption<Instant>,
        clock: C,
    }

    struct LeaderTimerTask<C: Clock> {
        weak_shared: Weak<Shared<C>>,
        next_heartbeat_time: SharedOption<Instant>,
        actor_client: actor::ActorClient,
        event: replica::LeaderTimerTick,
        clock: C,
    }

    impl LeaderTimerHandle {
        pub fn spawn_background_task(
            heartbeat_duration: Duration,
            actor_client: actor::ActorClient,
            peer_id: replica::ReplicaId,
            term: replica::Term,
        ) -> Self {
            Self::spawn_background_task_with_clock(heartbeat_duration, actor_client, peer_id, term, RealClock)
        }
    }

    impl<C: Clock + Send + Sync + 'static> LeaderTimerHandle<C> {
        // For tests
        pub(super) fn spawn_background_task_with_clock(
            heartbeat_duration: Duration,
            actor_client: actor::ActorClient,
            peer_id: replica::ReplicaId,
            term: replica::Term,
            clock: C,
        ) -> Self {
            let shared_opt = SharedOption::new();
            let shared = Arc::new(Shared {
                heartbeat_duration,
                next_heartbeat_time: shared_opt.clone(),
                clock: clock.clone(),
            });
            let event = replica::LeaderTimerTick { peer_id, term };

            let task = LeaderTimerTask {
                weak_shared: Arc::downgrade(&shared),
                next_heartbeat_time: shared_opt,
                actor_client,
                event,
                clock,
            };
            tokio::task::spawn(task.run());

            LeaderTimerHandle { shared }
        }

        /// This updates the timestamp when we will next notify the actor to send AE to this peer.
        pub fn reset_heartbeat_timer(&self) {
            self.shared.reset_heartbeat_timer();
        }
    }

    impl<C: Clock> Shared<C> {
        pub fn reset_heartbeat_timer(&self) {
            let new_timeout = self.clock.now() + self.heartbeat_duration;
            self.next_heartbeat_time.replace(new_timeout);
        }
    }

    impl<C: Clock> LeaderTimerTask<C> {
        pub async fn run(mut self) {
            // Notice: The first execution of the loop has an empty SharedOption, so the first iteration
            // will result in immediately publishing the timer event. This is ideal, because we want to
            // eagerly publish the  timer event to trigger leader to broadcast its heartbeat ASAP to the
            // newly established leader-follower pair (e.g. newly elected leader, new cluster member).
            loop {
                match self.next_heartbeat_time.take() {
                    Some(wake_time) => {
                        // We've sent a proactive heartbeat (due to new client request) to this peer,
                        // so we don't need periodic heartbeat until the next heartbeat duration.
                        self.clock.sleep_until(wake_time).await;
                    }
                    None => {
                        // We slept until the previous `next_heartbeat_time` and there's no updated
                        // `next_heartbeat_time`, which means we haven't sent a message to this peer
                        // in a while.

                        // Check if timer handle is still alive.
                        if let Some(shared) = self.weak_shared.upgrade() {
                            // Trigger a heartbeat. Reset heartbeat timer after the await.
                            self.actor_client.leader_timer(self.event.clone()).await;
                            shared.reset_heartbeat_timer();
                        } else {
                            // The timer handle has dropped, which means we are no longer a leader for
                            // the same term. Exit the task.
                            return;
                        }
                    }
                }
            }
        }
    }
}

mod follower {
    use crate::actor;
    use crate::replica::timers::shared_option::SharedOption;
    use crate::replica::timers::stop_signal;
    use crate::replica::timers::time::{Clock, RealClock};
    use rand::Rng;
    use std::ops::RangeInclusive;
    use tokio::time::{Duration, Instant};

    pub struct FollowerTimerHandle<C: Clock = RealClock> {
        next_wake_time: SharedOption<Instant>,
        timeout_range: RangeInclusive<Duration>,
        clock: C,
        _to_drop: stop_signal::Stopper,
    }

    struct FollowerTimerTask<C: Clock> {
        next_wake_time: SharedOption<Instant>,
        actor_client: actor::ActorClient,
        clock: C,
        stop_check: stop_signal::StopCheck,
        // timeout_backoff is an impl detail (not from paper) which is just a static amount of time
        // that this task will wait between triggering timeouts to the actor.
        timeout_backoff: Duration,
    }

    impl FollowerTimerHandle {
        pub fn spawn_background_task(
            min_timeout: Duration,
            max_timeout: Duration,
            actor_client: actor::ActorClient,
        ) -> Self {
            Self::spawn_background_task_with_clock(min_timeout, max_timeout, actor_client, RealClock)
        }
    }

    impl<C: Clock + Send + Sync + 'static> FollowerTimerHandle<C> {
        // For tests
        pub(super) fn spawn_background_task_with_clock(
            min_timeout: Duration,
            max_timeout: Duration,
            actor_client: actor::ActorClient,
            clock: C,
        ) -> Self {
            let shared_opt = SharedOption::new();
            let (stopper, stop_check) = stop_signal::new();

            let task = FollowerTimerTask {
                next_wake_time: shared_opt.clone(),
                actor_client,
                clock: clock.clone(),
                stop_check,
                timeout_backoff: min_timeout,
            };
            let handle = FollowerTimerHandle {
                next_wake_time: shared_opt,
                timeout_range: RangeInclusive::new(min_timeout, max_timeout),
                clock,
                _to_drop: stopper,
            };

            handle.reset_timeout();
            tokio::task::spawn(task.run());

            handle
        }

        pub fn reset_timeout(&self) {
            self.next_wake_time.replace(self.random_wake_time());
        }

        fn random_wake_time(&self) -> Instant {
            let rand_timeout = rand::thread_rng().gen_range(self.timeout_range.clone());
            self.clock.now() + rand_timeout
        }
    }

    impl<C: Clock + Send + Sync + 'static> FollowerTimerTask<C> {
        pub async fn run(mut self) {
            loop {
                match self.next_wake_time.take() {
                    Some(wake_time) => {
                        // We've received a leader heartbeat, so we sleep until the next timeout.
                        self.clock.sleep_until(wake_time).await;
                    }
                    None => {
                        // We slept until `wake_time` and didn't receive another message. If the queue
                        // is still open, it means we haven't heard from the leader and we should start
                        // a new election. Keep this task running until Actor drops the queue. In case
                        // actor concurrently received an AppendEntries call and remains as Follower.
                        if self.stop_check.should_stop() {
                            return;
                        }
                        self.actor_client.follower_timeout().await;
                        self.clock.sleep(self.timeout_backoff).await;
                    }
                }

                // The timer handle has dropped, which means we are no longer a follower for the
                // same term. Exit the task without starting a new election.
                if self.stop_check.should_stop() {
                    return;
                }
            }
        }
    }
}

mod shared_option {
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    pub struct SharedOption<T> {
        data: Arc<Mutex<Option<T>>>,
    }

    impl<T> SharedOption<T> {
        pub fn new() -> Self {
            SharedOption {
                data: Arc::new(Mutex::new(None)),
            }
        }

        pub fn replace(&self, new_data: T) {
            self.data
                .lock()
                .expect("SharedOption.replace() mutex guard poison")
                .replace(new_data);
        }

        pub fn take(&self) -> Option<T> {
            self.data.lock().expect("SharedOption.take() mutex guard poison").take()
        }
    }
}

mod time {
    use tokio::sync::watch;
    use tokio::time::{Duration, Instant};

    #[async_trait::async_trait]
    pub trait Clock: Clone {
        fn now(&self) -> Instant;
        async fn sleep_until(&mut self, deadline: Instant);

        async fn sleep(&mut self, duration: Duration) {
            let deadline = self.now() + duration;
            self.sleep_until(deadline).await;
        }
    }

    #[derive(Copy, Clone)]
    pub struct RealClock;

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
    pub fn mocked_clock() -> (MockClock, MockClockController) {
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
    pub struct MockClock {
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
    pub struct MockClockController {
        current_time: watch::Sender<Instant>,
        time_of_instantiation: Instant,
    }

    #[allow(dead_code)]
    impl MockClockController {
        pub fn current_time(&self) -> Instant {
            *self.current_time.borrow()
        }

        pub fn elapsed_time(&self) -> Duration {
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
        pub fn advance(&mut self, duration: Duration) {
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
}

mod stop_signal {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    pub struct Stopper {
        stop_signal: Arc<AtomicBool>,
    }

    pub struct StopCheck {
        stop_signal: Arc<AtomicBool>,
    }

    impl Drop for Stopper {
        fn drop(&mut self) {
            self.stop_signal.store(true, Ordering::Release);
        }
    }

    impl StopCheck {
        pub fn should_stop(&self) -> bool {
            self.stop_signal.load(Ordering::Acquire)
        }
    }

    pub fn new() -> (Stopper, StopCheck) {
        let stop_signal = Arc::new(AtomicBool::new(false));

        let stopper = Stopper {
            stop_signal: stop_signal.clone(),
        };
        let stop_check = StopCheck { stop_signal };

        (stopper, stop_check)
    }
}

#[cfg(test)]
mod integ_tests {
    use crate::actor::{ActorClient, Event};
    use crate::replica::timers::LeaderTimerHandle;
    use crate::replica::timers::{time, FollowerTimerHandle};
    use crate::replica::{LeaderTimerTick, ReplicaId, Term};
    use std::fmt::Debug;
    use tokio::sync::mpsc;
    use tokio::time::Duration;

    struct TestUtilReceiver<T> {
        rx: mpsc::Receiver<T>,
    }

    impl<T: Debug> TestUtilReceiver<T> {
        pub fn new(rx: mpsc::Receiver<T>) -> Self {
            TestUtilReceiver { rx }
        }

        pub async fn recv(&mut self) -> T {
            self.recv_with_sanity_timeout().await.expect("Expected value")
        }

        pub async fn recv_assert_empty_and_closed(&mut self) {
            if let Some(_) = self.recv_with_sanity_timeout().await {
                panic!("Expected None");
            }
        }

        async fn recv_with_sanity_timeout(&mut self) -> Option<T> {
            tokio::time::timeout(Duration::from_secs(5), self.rx.recv())
                .await
                .expect("Unexpected timeout")
        }

        pub async fn recv_assert_timeout(&mut self, timeout: Duration) {
            tokio::time::timeout(timeout, self.rx.recv())
                .await
                .expect_err("Expected timeout");
        }
    }

    struct TestUtilActor {
        receiver: TestUtilReceiver<Event>,
        timeout: Duration,
    }

    impl TestUtilActor {
        pub fn new(actor_queue_rx: mpsc::Receiver<Event>) -> Self {
            TestUtilActor {
                receiver: TestUtilReceiver::new(actor_queue_rx),
                timeout: Duration::from_millis(10),
            }
        }

        pub async fn assert_leader_heartbeat_event(&mut self, expected_leader_heartbeat: LeaderTimerTick) {
            if let Event::LeaderTimer(event) = self.receiver.recv().await {
                assert_eq!(event, expected_leader_heartbeat);
            } else {
                panic!("Unexpected event");
            }
        }

        pub async fn assert_follower_timeout_event(&mut self) {
            if let Event::FollowerTimeout = self.receiver.recv().await {
                // Success!
            } else {
                panic!("Unexpected event");
            }
        }

        pub async fn assert_no_event(&mut self) {
            self.receiver.recv_assert_timeout(self.timeout).await;
        }
    }

    #[tokio::test]
    async fn leader_timer_handle_lifecycle() {
        // -- setup --
        let heartbeat_timeout = Duration::from_millis(100);
        let (tx, rx) = mpsc::channel(10);
        let actor_client = ActorClient::new(tx);
        let mut actor = TestUtilActor::new(rx);

        let peer_id = ReplicaId::new("peer-123");
        let term = Term::new(10);
        let expected_heartbeat_event = LeaderTimerTick {
            peer_id: peer_id.clone(),
            term,
        };

        let (mock_clock, mut mock_clock_controller) = time::mocked_clock();

        // -- execute & verify --

        // 1. Spawn task, assert there is one event in the queue.
        let timer_handle = LeaderTimerHandle::spawn_background_task_with_clock(
            heartbeat_timeout,
            actor_client,
            peer_id,
            term,
            mock_clock,
        );

        actor
            .assert_leader_heartbeat_event(expected_heartbeat_event.clone())
            .await;
        actor.assert_no_event().await;

        // 2. Advance time and receive heartbeat many times
        for _ in 0..5 {
            mock_clock_controller.advance(heartbeat_timeout);
            actor
                .assert_leader_heartbeat_event(expected_heartbeat_event.clone())
                .await;
            actor.assert_no_event().await;
        }

        // 3. Advance time by a big leap, still receive single heartbeat
        mock_clock_controller.advance(heartbeat_timeout * 5);
        actor
            .assert_leader_heartbeat_event(expected_heartbeat_event.clone())
            .await;
        actor.assert_no_event().await;

        // 4. Drop handle and assert closed
        drop(timer_handle);
        mock_clock_controller.advance(heartbeat_timeout);
        actor.receiver.recv_assert_empty_and_closed().await;
    }

    #[tokio::test]
    async fn leader_timer_handle_resetting_timeout() {
        // -- setup --
        let heartbeat_timeout = Duration::from_millis(100);
        let (tx, rx) = mpsc::channel(10);
        let actor_client = ActorClient::new(tx);
        let mut actor = TestUtilActor::new(rx);

        let peer_id = ReplicaId::new("peer-123");
        let term = Term::new(10);
        let expected_heartbeat_event = LeaderTimerTick {
            peer_id: peer_id.clone(),
            term,
        };

        let (mock_clock, mut mock_clock_controller) = time::mocked_clock();

        // -- execute & verify --

        // 1. Spawn task, assert there is one event in the queue.
        let timer_handle = LeaderTimerHandle::spawn_background_task_with_clock(
            heartbeat_timeout,
            actor_client,
            peer_id,
            term,
            mock_clock,
        );

        actor
            .assert_leader_heartbeat_event(expected_heartbeat_event.clone())
            .await;
        actor.assert_no_event().await;

        // 2a. Repeatedly advance time by 0.5 and reset heartbeat timer
        for _ in 0..5 {
            mock_clock_controller.advance(heartbeat_timeout / 2);
            timer_handle.reset_heartbeat_timer();
        }
        // 2b. Assert no heartbeat (because we reset it!).
        actor.assert_no_event().await;

        // Sanity check that T=2.5.
        // Notice heartbeat timeout should be set for T=3.5.
        assert_eq!(mock_clock_controller.elapsed_time(), heartbeat_timeout * 5 / 2);

        // 3a. Advance time to T=3, assert no heartbeat (because we reset it!)
        mock_clock_controller.advance(heartbeat_timeout / 2);
        actor.assert_no_event().await;

        // 3b. Advance time to T=3.5, assert heartbeat (timeout occurs)
        mock_clock_controller.advance(heartbeat_timeout / 2);
        actor
            .assert_leader_heartbeat_event(expected_heartbeat_event.clone())
            .await;

        // Sanity check that T=3.5
        assert_eq!(mock_clock_controller.elapsed_time(), heartbeat_timeout * 7 / 2);
    }

    #[tokio::test]
    async fn follower_timer_handle_reset_and_timeout() {
        // -- setup --
        let timeout = Duration::from_millis(100);
        let (tx, rx) = mpsc::channel(10);
        let actor_client = ActorClient::new(tx);
        let mut actor = TestUtilActor::new(rx);

        let (mock_clock, mut mock_clock_controller) = time::mocked_clock();

        // -- execute & verify --

        // 1. Spawn task, assert there is no event in the queue.
        let timer_handle = FollowerTimerHandle::spawn_background_task_with_clock(
            // We are not testing randomness/jitter, so make min/max the same.
            /* min */ timeout,
            /* max */ timeout,
            actor_client,
            mock_clock,
        );

        actor.assert_no_event().await;

        // 2. Advance time and reset timeout many times, assert no event
        for _ in 0..5 {
            mock_clock_controller.advance(timeout / 2);
            timer_handle.reset_timeout();
        }
        actor.assert_no_event().await;

        // Sanity check T=2.5
        assert_eq!(mock_clock_controller.elapsed_time(), timeout * 5 / 2);

        // 3. Validate no timeout occurs at T < 3.5
        let one_ns = Duration::from_nanos(1);
        mock_clock_controller.advance(timeout - one_ns);
        actor.assert_no_event().await;

        // 4. Validate timeout occurs and timer task exits at exactly T >= 3.5, because our last
        // timeout reset was at T=2.5.
        mock_clock_controller.advance(one_ns);
        actor.assert_follower_timeout_event().await;
    }

    #[tokio::test]
    async fn follower_timer_handle_drop() {
        // -- setup --
        let timeout = Duration::from_millis(100);
        let (tx, rx) = mpsc::channel(10);
        let actor_client = ActorClient::new(tx);
        let mut actor = TestUtilActor::new(rx);

        let (mock_clock, mut mock_clock_controller) = time::mocked_clock();

        // -- execute --
        // Spawn task, it will be sleeping. Drop handle to observe behavior.
        let timer_handle = FollowerTimerHandle::spawn_background_task_with_clock(
            // We are not testing randomness/jitter, so make min/max the same.
            /* min */ timeout,
            /* max */ timeout,
            actor_client,
            mock_clock,
        );
        drop(timer_handle);

        // -- verify --
        // Fast-fwd time to ensure timer task would've fired an event, and then assert timer task
        // has exited.
        mock_clock_controller.advance(timeout * 2);
        actor.receiver.recv_assert_empty_and_closed().await;
    }

    /// TDD: Fixes bug of previous impl. That's why this test looks oddly specific and minimal.
    #[tokio::test]
    async fn follower_timer_handle_reset_timeout_after_timer_task_exit() {
        // -- setup --
        let timeout = Duration::from_millis(100);
        let (tx, rx) = mpsc::channel(10);
        let actor_client = ActorClient::new(tx);
        let mut actor = TestUtilActor::new(rx);

        let (mock_clock, mut mock_clock_controller) = time::mocked_clock();

        // Spawn task, assert there is no event in the queue.
        let timer_handle = FollowerTimerHandle::spawn_background_task_with_clock(
            // We are not testing randomness/jitter, so make min/max the same.
            /* min */ timeout,
            /* max */ timeout,
            actor_client,
            mock_clock,
        );
        actor.assert_no_event().await;

        // -- execute --
        // Trigger timeout (causes timer task to exit), attempt to reset timeout. This might appear
        // as though the ordering is impossible for the single-threaded actor to observe. But it is
        // possible for timer task to notify actor queue of a timeout while there is already an
        // AppendEntries (i.e. reset_timeout) message is in the actor queue. We wait for actor to
        // the receive timeout first here because that's the only point of synchronization we have
        // available to ensure the task has triggered a timeout.
        mock_clock_controller.advance(timeout);
        actor.assert_follower_timeout_event().await;
        timer_handle.reset_timeout();

        // -- verify --
        // Implicit assertion: no panic
        // Validate timer task is still running
        for _ in 0..5 {
            mock_clock_controller.advance(timeout / 2);
            timer_handle.reset_timeout();
        }
        actor.assert_no_event().await;
    }
}
