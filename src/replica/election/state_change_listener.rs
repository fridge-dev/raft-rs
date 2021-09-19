use crate::replica::LeaderRedirectInfo;
use tokio::sync::watch;

#[derive(Clone, Debug)]
pub(crate) enum ElectionStateSnapshot {
    Leader,
    Candidate,
    Follower(LeaderRedirectInfo),
    FollowerNoLeader,
}

pub(super) fn new(initial_state: ElectionStateSnapshot) -> (ElectionStateChangeNotifier, ElectionStateChangeListener) {
    let (snd, rcv) = watch::channel(initial_state);

    (ElectionStateChangeNotifier { snd }, ElectionStateChangeListener { rcv })
}

pub(super) struct ElectionStateChangeNotifier {
    snd: watch::Sender<ElectionStateSnapshot>,
}

impl ElectionStateChangeNotifier {
    pub(super) fn notify_new_state(&self, new_state: ElectionStateSnapshot) {
        let _ = self.snd.send(new_state);
    }
}

#[derive(Clone)]
pub(crate) struct ElectionStateChangeListener {
    rcv: watch::Receiver<ElectionStateSnapshot>,
}

impl ElectionStateChangeListener {
    pub(crate) async fn next(&mut self) -> Option<ElectionStateSnapshot> {
        match self.rcv.changed().await {
            Ok(_) => Some(self.rcv.borrow().clone()),
            Err(_) => None,
        }
    }
}
