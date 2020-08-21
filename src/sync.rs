use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::sync::mpsc;

pub struct UnboundedSink<T>(mpsc::UnboundedSender<T>);

impl<T> Clone for UnboundedSink<T> {
    fn clone(&self) -> Self {
        UnboundedSink(self.0.clone())
    }
}

impl<T> UnboundedSink<T> {
    pub fn new(inner: mpsc::UnboundedSender<T>) -> Self {
        UnboundedSink(inner)
    }
}

impl<T> futures::Sink<T> for UnboundedSink<T> {
    type Error = mpsc::error::SendError<T>;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, msg: T) -> Result<(), Self::Error> {
        self.0.send(msg)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
