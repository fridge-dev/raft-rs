use crate::actor;
use crate::replica;
use rand::Rng;
use std::ops::RangeInclusive;
use tokio::time::{Duration, Instant};
use std::sync::{Mutex, Arc, Weak};

pub struct LeaderTimerHandle {
    state: Arc<LeaderTimerHandleState>,
}

struct LeaderTimerHandleState {
    heartbeat_duration: Duration,
    next_heartbeat_time: SharedOption<Instant>,
}

impl LeaderTimerHandleState {
    pub fn reset_heartbeat_timer(&self) {
        self.next_heartbeat_time.replace(Instant::now() + self.heartbeat_duration);
    }
}

impl LeaderTimerHandle {
    pub fn spawn_background_task(
        heartbeat_duration: Duration,
        actor_client: actor::ActorClient,
        peer_id: replica::ReplicaId,
        term: replica::Term,
    ) -> Self {
        let shared_opt = SharedOption::new();
        let handle = Arc::new(LeaderTimerHandleState {
            heartbeat_duration,
            next_heartbeat_time: shared_opt.clone(),
        });
        let event = replica::LeaderTimerTick { peer_id, term };

        tokio::task::spawn(Self::leader_timer_task(
            Arc::downgrade(&handle),
            shared_opt.clone(),
            actor_client,
            event,
        ));

        LeaderTimerHandle {
            state: handle,
        }
    }

    pub fn reset_heartbeat_timer(&self) {
        self.state.reset_heartbeat_timer();
    }

    async fn leader_timer_task(
        weak_handle: Weak<LeaderTimerHandleState>,
        next_heartbeat_time: SharedOption<Instant>,
        actor_client: actor::ActorClient,
        event: replica::LeaderTimerTick,
    ) {
        // Eagerly publish timer event before entering first tick loop. This will trigger newly
        // elected leader to broadcast its heartbeat sooner than the heartbeat duration.
        actor_client.leader_timer(event.clone()).await;

        loop {
            match next_heartbeat_time.take() {
                Some(wake_time) => {
                    // We've sent a proactive heartbeat (due to new client request) to this peer,
                    // so we don't need periodic heartbeat until the next heartbeat duration.
                    tokio::time::sleep_until(wake_time).await;
                }
                None => {
                    // We slept until the previous `next_heartbeat_time` and there's no updated
                    // `next_heartbeat_time`, which means we haven't sent a message to this peer
                    // in a while.

                    // First check if timer handle is still alive, and reset heartbeat timer if so.
                    if let Some(handle) = weak_handle.upgrade() {
                        handle.reset_heartbeat_timer();
                    } else {
                        // The timer handle has dropped, which means we are no longer a leader for
                        // the same term. Exit the task.
                        return;
                    }

                    // Trigger a heartbeat.
                    actor_client.leader_timer(event.clone()).await;
                }
            }
        }
    }
}

pub struct FollowerTimerHandle {
    // We use flume instead of tokio because we need non-blocking try_recv() and tokio's was removed in 1.0
    // TODO:2 optimize to use `SharedOption`
    wake_time_queue: flume::Sender<Instant>,
    timeout_range: RangeInclusive<Duration>,
}

impl FollowerTimerHandle {
    pub fn spawn_background_task(
        min_timeout: Duration,
        max_timeout: Duration,
        actor_client: actor::ActorClient,
    ) -> Self {
        let (tx, rx) = flume::unbounded();

        let handle = FollowerTimerHandle {
            wake_time_queue: tx,
            timeout_range: RangeInclusive::new(min_timeout, max_timeout),
        };
        handle.reset_timeout();

        tokio::task::spawn(Self::follower_timer_task(rx, actor_client));

        handle
    }

    pub fn reset_timeout(&self) {
        match self.wake_time_queue.try_send(self.random_wake_time()) {
            Ok(_) => {}
            Err(flume::TrySendError::Disconnected(_)) => {
                panic!("Follower Timeout task receiver should never stop until we close the queue. Bug wtf?")
            }
            Err(flume::TrySendError::Full(_)) => {
                panic!("We're using unbounded queue. This should never happen. #FollowerTimeoutTask")
            }
        }
    }

    fn random_wake_time(&self) -> Instant {
        let rand_timeout = rand::thread_rng().gen_range(self.timeout_range.clone());
        Instant::now() + rand_timeout
    }

    async fn follower_timer_task(queue: flume::Receiver<Instant>, actor_client: actor::ActorClient) {
        loop {
            match queue.try_recv() {
                Ok(wake_time) => {
                    // We've received a leader heartbeat, so we sleep until the next timeout.
                    tokio::time::sleep_until(wake_time).await;
                }
                Err(flume::TryRecvError::Empty) => {
                    // We slept until `wake_time` and didn't receive another message. If the queue
                    // is still open, it means we haven't heard from the leader and we should start
                    // a new election.
                    actor_client.follower_timeout().await;
                    return;
                }
                Err(flume::TryRecvError::Disconnected) => {
                    // The timer handle has dropped, which means we are no longer a follower for the
                    // same term. Exit the task without starting a new election.
                    return;
                }
            }
        }
    }
}

#[derive(Clone, Default)]
struct SharedOption<T> {
    data: Arc<Mutex<Option<T>>>
}

impl<T> SharedOption<T> {
    pub fn new() -> Self {
        SharedOption {
            data: Arc::new(Mutex::new(None)),
        }
    }

    pub fn replace(&self, new_data: T) {
        self.data.lock().expect("SharedOption.replace() mutex guard poison").replace(new_data);
    }

    pub fn take(&self) -> Option<T> {
        self.data.lock().expect("SharedOption.take() mutex guard poison").take()
    }
}

#[allow(dead_code)] // keeping around as a reference
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

// TODO:1.5 tests to validate that timers do what we expect
