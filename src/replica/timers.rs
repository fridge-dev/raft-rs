use crate::actor;
use crate::replica;
use rand::Rng;
use std::ops::RangeInclusive;
use tokio::time;
use tokio::time::{Duration, Instant};

pub struct LeaderTimerHandle {
    // will be dropped
    _stopper: stop_signal::Stopper,
}

impl LeaderTimerHandle {
    pub fn spawn_background_task(
        heartbeat_duration: Duration,
        actor_client: actor::ActorClient,
        peer_id: replica::ReplicaId,
        term: replica::Term,
    ) -> Self {
        let (stopper, stop_check) = stop_signal::new();

        tokio::task::spawn(Self::leader_timer_task(
            stop_check,
            heartbeat_duration,
            actor_client,
            peer_id,
            term,
        ));

        LeaderTimerHandle { _stopper: stopper }
    }

    async fn leader_timer_task(
        stop_check: stop_signal::StopCheck,
        heartbeat_duration: Duration,
        actor_client: actor::ActorClient,
        peer_id: replica::ReplicaId,
        term: replica::Term,
    ) {
        let event = replica::LeaderTimerTick {
            peer_id: peer_id.clone(),
            term
        };

        // Eagerly publish timer event before entering first tick loop. This will trigger newly
        // elected leader to broadcast its heartbeat sooner than the heartbeat duration.
        actor_client.leader_timer(event.clone()).await;

        let mut interval = time::interval(heartbeat_duration);
        loop {
            interval.tick().await;
            if stop_check.should_stop() {
                break;
            }
            actor_client.leader_timer(event.clone()).await;
        }
    }
}

pub struct FollowerTimerHandle {
    queue: flume::Sender<Instant>,
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
            queue: tx,
            timeout_range: RangeInclusive::new(min_timeout, max_timeout),
        };
        handle.reset_timeout();

        tokio::task::spawn(Self::follower_timer_task(rx, actor_client));

        handle
    }

    pub fn reset_timeout(&self) {
        match self.queue.try_send(self.random_timeout()) {
            Ok(_) => {}
            Err(flume::TrySendError::Disconnected(_)) => {
                panic!("Follower Timeout task receiver should never stop until we close the queue. Bug wtf?")
            }
            Err(flume::TrySendError::Full(_)) => {
                panic!("We're using unbounded queue. This should never happen. #FollowerTimeoutTask")
            }
        }
    }

    fn random_timeout(&self) -> Instant {
        let rand_timeout = rand::thread_rng().gen_range(self.timeout_range.clone());
        Instant::now() + rand_timeout
    }

    async fn follower_timer_task(queue: flume::Receiver<Instant>, actor_client: actor::ActorClient) {
        loop {
            match queue.try_recv() {
                Ok(wake_time) => {
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
                    return;
                }
            }
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
