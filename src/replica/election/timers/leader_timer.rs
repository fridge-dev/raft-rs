use crate::replica::election::timers::shared_option::SharedOption;
use crate::replica::election::timers::time::{Clock, RealClock};
use crate::{actor, replica};
use std::sync::{Arc, Weak};
use tokio::time::{Duration, Instant};

pub(crate) struct LeaderTimerHandle<C: Clock = RealClock> {
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
    actor_client: actor::WeakActorClient,
    event: replica::LeaderTimerTick,
    clock: C,
}

impl LeaderTimerHandle {
    pub(crate) fn spawn_timer_task(
        heartbeat_duration: Duration,
        actor_client: actor::WeakActorClient,
        peer_id: replica::ReplicaId,
        term: replica::Term,
    ) -> Self {
        // Add minimal logic in this constructor, as it is untested.
        let (task, handle) = LeaderTimerTask::new(heartbeat_duration, actor_client, peer_id, term, RealClock);
        tokio::task::spawn(task.run());

        handle
    }
}

impl<C: Clock + Send + Sync + 'static> LeaderTimerHandle<C> {
    /// This updates the timestamp when we will next notify the actor to send AE to this peer.
    pub(crate) fn reset_heartbeat_timer(&self) {
        self.shared.reset_heartbeat_timer();
    }
}

impl<C: Clock> Shared<C> {
    fn reset_heartbeat_timer(&self) {
        let new_timeout = self.clock.now() + self.heartbeat_duration;
        self.next_heartbeat_time.replace(new_timeout);
    }
}

impl<C: Clock> LeaderTimerTask<C> {
    fn new(
        heartbeat_duration: Duration,
        actor_client: actor::WeakActorClient,
        peer_id: replica::ReplicaId,
        term: replica::Term,
        clock: C,
    ) -> (Self, LeaderTimerHandle<C>) {
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
        let handle = LeaderTimerHandle { shared };

        (task, handle)
    }

    async fn run(mut self) {
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
                        let _ = self.actor_client.leader_timer(self.event.clone()).await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorClient;
    use crate::replica::election::timers::test_utils::TestUtilActor;
    use crate::replica::election::timers::time;
    use crate::replica::{LeaderTimerTick, ReplicaId, Term};
    use std::time::Duration;

    #[tokio::test]
    async fn leader_timer_handle_lifecycle() {
        // -- setup --
        let heartbeat_timeout = Duration::from_millis(100);
        let (strong_actor_client, rx) = ActorClient::new(10);
        let actor_client = strong_actor_client.weak();
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
        let (timer_task, timer_handle) =
            LeaderTimerTask::new(heartbeat_timeout, actor_client, peer_id, term, mock_clock);
        let task_join_handle = tokio::task::spawn(timer_task.run());

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

        // 4. Drop handle and assert timer task exited without sending more events.
        drop(timer_handle);
        mock_clock_controller.advance(heartbeat_timeout);
        task_join_handle.await.unwrap();
        actor.assert_no_event().await;
    }

    #[tokio::test]
    async fn leader_timer_handle_resetting_timeout() {
        // -- setup --
        let heartbeat_timeout = Duration::from_millis(100);
        let (strong_actor_client, rx) = ActorClient::new(10);
        let actor_client = strong_actor_client.weak();
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
        let (timer_task, timer_handle) =
            LeaderTimerTask::new(heartbeat_timeout, actor_client, peer_id, term, mock_clock);
        tokio::task::spawn(timer_task.run());

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
}
