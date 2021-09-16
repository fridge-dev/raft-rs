use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

pub(crate) fn shutdown_signal() -> (RpcServerShutdownHandle, RpcServerShutdownSignal) {
    let (tx, rx) = oneshot::channel();

    (RpcServerShutdownHandle { _tx: tx }, RpcServerShutdownSignal { rx })
}

pub(crate) struct RpcServerShutdownHandle {
    _tx: oneshot::Sender<()>,
}

pub(crate) struct RpcServerShutdownSignal {
    rx: oneshot::Receiver<()>,
}

impl Future for RpcServerShutdownSignal {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let rx = Pin::new(&mut self.rx);

        match rx.poll(cx) {
            Poll::Pending => Poll::Pending,
            // We don't care if oneshot Sender sent value or dropped
            Poll::Ready(_) => Poll::Ready(()),
        }
    }
}
