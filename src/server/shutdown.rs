use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use tokio::sync::oneshot;

pub fn shutdown_signal() -> (RpcServerShutdownHandle, RpcServerShutdownSignal) {
    let (tx, rx) = oneshot::channel();

    (RpcServerShutdownHandle { _tx: tx }, RpcServerShutdownSignal { rx })
}

pub struct RpcServerShutdownHandle {
    _tx: oneshot::Sender<()>,
}

pub struct RpcServerShutdownSignal {
    rx: oneshot::Receiver<()>,
}

// To help my future self remember how this works.
type IgnoredType = Result<(), oneshot::error::RecvError>;

impl Future for RpcServerShutdownSignal {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let rx = Pin::new(&mut self.rx);
        // We don't care if oneshot Sender sent value or dropped
        let _: IgnoredType = ready!(rx.poll(cx));
        Poll::Ready(())
    }
}
