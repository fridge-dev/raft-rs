use crate::actor::Event;
use crate::replica::LeaderTimerTick;
use std::fmt::Debug;
use std::time::Duration;
use tokio::sync::mpsc;

struct TestUtilReceiver<T> {
    rx: mpsc::Receiver<T>,
}

impl<T: Debug> TestUtilReceiver<T> {
    pub(super) fn new(rx: mpsc::Receiver<T>) -> Self {
        TestUtilReceiver { rx }
    }

    pub(super) async fn recv(&mut self) -> T {
        self.recv_with_sanity_timeout().await.expect("Expected value")
    }

    #[allow(dead_code)]
    pub(super) async fn recv_assert_empty_and_closed(&mut self) {
        if let Some(_) = self.recv_with_sanity_timeout().await {
            panic!("Expected None");
        }
    }

    async fn recv_with_sanity_timeout(&mut self) -> Option<T> {
        tokio::time::timeout(Duration::from_secs(5), self.rx.recv())
            .await
            .expect("Unexpected timeout")
    }

    pub(super) async fn recv_assert_timeout(&mut self, timeout: Duration) {
        tokio::time::timeout(timeout, self.rx.recv())
            .await
            .expect_err("Expected timeout");
    }
}

pub(super) struct TestUtilActor {
    receiver: TestUtilReceiver<Event>,
    timeout: Duration,
}

impl TestUtilActor {
    pub(super) fn new(actor_queue_rx: mpsc::Receiver<Event>) -> Self {
        TestUtilActor {
            receiver: TestUtilReceiver::new(actor_queue_rx),
            timeout: Duration::from_millis(10),
        }
    }

    pub(super) async fn assert_leader_heartbeat_event(&mut self, expected_leader_heartbeat: LeaderTimerTick) {
        if let Event::LeaderTimer(event) = self.receiver.recv().await {
            assert_eq!(event, expected_leader_heartbeat);
        } else {
            panic!("Unexpected event");
        }
    }

    pub(super) async fn assert_follower_timeout_event(&mut self) {
        if let Event::FollowerTimeout = self.receiver.recv().await {
            // Success!
        } else {
            panic!("Unexpected event");
        }
    }

    pub(super) async fn assert_no_event(&mut self) {
        self.receiver.recv_assert_timeout(self.timeout).await;
    }
}
