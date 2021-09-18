use crate::actor;
use crate::replica::election::timers::shared_option::SharedOption;
use crate::replica::election::timers::stop_signal;
use crate::replica::election::timers::time::{Clock, RealClock};
use rand::Rng;
use std::ops::RangeInclusive;
use tokio::time::{Duration, Instant};

pub(crate) struct FollowerTimerHandle<C: Clock = RealClock> {
    next_wake_time: SharedOption<Instant>,
    timeout_range: RangeInclusive<Duration>,
    clock: C,
    _to_drop: stop_signal::Stopper,
}

struct FollowerTimerTask<C: Clock> {
    next_wake_time: SharedOption<Instant>,
    actor_client: actor::WeakActorClient,
    clock: C,
    stop_check: stop_signal::StopCheck,
    // timeout_backoff is an impl detail (not from paper) which is just a static amount of time
    // that this task will wait between triggering timeouts to the actor.
    timeout_backoff: Duration,
}

impl FollowerTimerHandle {
    pub(crate) fn spawn_timer_task(
        min_timeout: Duration,
        max_timeout: Duration,
        actor_client: actor::WeakActorClient,
    ) -> Self {
        let (task, handle) = FollowerTimerTask::new(min_timeout, max_timeout, actor_client, RealClock);
        tokio::task::spawn(task.run());

        handle
    }
}

impl<C: Clock + Send + Sync + 'static> FollowerTimerHandle<C> {
    pub(crate) fn reset_timeout(&self) {
        self.next_wake_time.replace(self.random_wake_time());
    }

    fn random_wake_time(&self) -> Instant {
        let rand_timeout = rand::thread_rng().gen_range(self.timeout_range.clone());
        self.clock.now() + rand_timeout
    }
}

impl<C: Clock + Send + Sync + 'static> FollowerTimerTask<C> {
    fn new(
        min_timeout: Duration,
        max_timeout: Duration,
        actor_client: actor::WeakActorClient,
        clock: C,
    ) -> (Self, FollowerTimerHandle<C>) {
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

        // Timer task must have a timeout value present when it starts, otherwise it may
        // trigger a timeout immediately after we become a follower.
        handle.reset_timeout();

        (task, handle)
    }

    async fn run(mut self) {
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
                    let _ = self.actor_client.follower_timeout().await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorClient;
    use crate::replica::election::timers::test_utils::TestUtilActor;
    use crate::replica::election::timers::time;

    #[tokio::test]
    async fn follower_timer_handle_reset_and_timeout() {
        // -- setup --
        let timeout = Duration::from_millis(100);
        let (strong_actor_client, rx) = ActorClient::new(10);
        let actor_client = strong_actor_client.weak();
        let mut actor = TestUtilActor::new(rx);

        let (mock_clock, mut mock_clock_controller) = time::mocked_clock();

        // -- execute & verify --

        // 1. Spawn task, assert there is no event in the queue.
        let (timer_task, timer_handle) = FollowerTimerTask::new(
            // We are not testing randomness/jitter, so make min/max the same.
            /* min */ timeout,
            /* max */ timeout,
            actor_client,
            mock_clock,
        );
        tokio::task::spawn(timer_task.run());

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
        let (strong_actor_client, rx) = ActorClient::new(10);
        let actor_client = strong_actor_client.weak();
        let mut actor = TestUtilActor::new(rx);

        let (mock_clock, mut mock_clock_controller) = time::mocked_clock();

        // -- execute --
        // Spawn task, it will be sleeping. Drop handle to observe behavior.
        let (timer_task, timer_handle) = FollowerTimerTask::new(
            // We are not testing randomness/jitter, so make min/max the same.
            /* min */ timeout,
            /* max */ timeout,
            actor_client,
            mock_clock,
        );
        let task_join_handle = tokio::task::spawn(timer_task.run());
        drop(timer_handle);

        // -- verify --
        // Fast-fwd time to ensure timer task would've fired an event, and then assert timer task
        // has exited.
        mock_clock_controller.advance(timeout * 2);
        task_join_handle.await.unwrap();
        actor.assert_no_event().await;
    }

    /// TDD: Fixes bug of previous impl. That's why this test looks oddly specific and minimal.
    #[tokio::test]
    async fn follower_timer_handle_reset_timeout_after_timer_task_exit() {
        // -- setup --
        let timeout = Duration::from_millis(100);
        let (strong_actor_client, rx) = ActorClient::new(10);
        let actor_client = strong_actor_client.weak();
        let mut actor = TestUtilActor::new(rx);

        let (mock_clock, mut mock_clock_controller) = time::mocked_clock();

        // Spawn task, assert there is no event in the queue.
        let (timer_task, timer_handle) = FollowerTimerTask::new(
            // We are not testing randomness/jitter, so make min/max the same.
            /* min */ timeout,
            /* max */ timeout,
            actor_client,
            mock_clock,
        );
        tokio::task::spawn(timer_task.run());
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
