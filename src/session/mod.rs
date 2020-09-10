mod events;
mod id;
mod map;

use std::collections::hash_map;
use std::fmt;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use async_trait::async_trait;
use futures::prelude::*;
use futures::select_biased;
use futures::stream::{SplitSink, SplitStream};
use log::{debug, error, info};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::pin;
use tokio::sync::{watch, Notify};
use tokio_tungstenite::WebSocketStream;
use tungstenite::error::Error as WsError;
use tungstenite::protocol::Message;

use self::events::{Event, EventPipe, EventSinkCtx, HandoffPacket};
use self::id::SessionId;
use self::map::EventSinkEntry;

#[derive(Debug, Error)]
pub enum LifecycleError {
    #[error("unable to send event to session")]
    EventSinkClosed,
    #[error("unable to send update to event sink map")]
    MapUpdaterClosed,
    #[error("unable to send closing notifier to takeover session")]
    HandoffChannelClosed,
    #[error("unable to handoff due to error in old session")]
    HandoffCancelled,
}

type TcpWebSocketStream = WebSocketStream<TcpStream>;
pub type MessageSink = SplitSink<TcpWebSocketStream, Message>;

#[async_trait]
pub trait Session: 'static + fmt::Debug + Sized {
    type Key: 'static + Copy + fmt::Debug + Eq + Ord + Hash + Send + Sync + Unpin;
    type Message: 'static + fmt::Debug + Send + Sync;

    fn get_key(&self) -> Self::Key;

    async fn handle_open(&mut self, sink: &mut MessageSink);
    async fn handle_close(&mut self, sink: &mut MessageSink);
    async fn handle_message(&mut self, sink: &mut MessageSink, msg: Message);
}

pub struct Ctx<T: Session> {
    id: SessionId,
    session: T,
    notify: Arc<Notify>,
    sink: SplitSink<TcpWebSocketStream, Message>,
}

impl<T: Session> Ctx<T> {
    async fn run(mut self, mut stream: SplitStream<TcpWebSocketStream>) -> anyhow::Result<()> {
        let (event_pipe, map_updater) = handoff_or_make_pipe(self.id, &self.session).await?;
        let (mut event_sink, mut event_stream) = event_pipe;

        loop {
            pin! {
                let next_message = stream.next().fuse();
                let next_event = event_stream.next().fuse();
            }

            select_biased! {
                res = next_message => match res {
                    None => break Ok(()),
                    Some(msg) => {
                        event_sink
                            .send(Event::Message(msg?))
                            .await?;
                    }
                },

                res = next_event => match res {
                    None => break Ok(()),
                    Some(evt) => match evt {
                        Event::Message(msg) => {
                            self.session.handle_message(&mut self.sink, msg).await;
                        },
                        Event::Event(e) => {
                            let _ = e;
                            unimplemented!()
                        },
                        Event::Handoff(chan) => {
                            // First, make anyone that asks for this event sink wait for handoff.
                            let handoff_complete = self.notify.clone();
                            let packet = map_updater.broadcast(None)
                                .map(move |_| HandoffPacket {
                                    event_stream,
                                    map_updater,
                                    handoff_complete,
                                });

                            let (packet, err) = match packet {
                                Ok(packet) => (Some(packet), Ok(())),
                                Err(_) => (None, Err(LifecycleError::MapUpdaterClosed)),
                            };

                            err.and(chan.send(packet).map_err(|_| LifecycleError::HandoffChannelClosed))?;
                            return Ok(());
                        },
                    },
                },
            }
        }
    }
}

async fn handoff_or_make_pipe<T: Session>(
    id: SessionId,
    session: &T,
) -> anyhow::Result<(EventPipe<T>, watch::Sender<Option<EventSinkCtx<T>>>)> {
    let key = session.get_key();
    let event_sink_map = map::get_or_create_event_sink_map().await;
    let mut old_sink_ctx = {
        let unlocked_map = event_sink_map.read().await;
        match unlocked_map.get(&key) {
            None => None,
            Some(entry) => entry.get().await.ok(),
        }
    };

    //  map_updater: A tokio watch channel that broadcasts updates to the map.  Accepts None to
    //    block all interested parties until the handoff is complete, and Some(EventSinkCtx) to
    //    update/unblock.  The public interface to the event sink map acts like a future that
    //    doesn't resolve unless Some was most recently sent to the watch.
    //  old_event_pipe: The event sink/stream pair from the previous session.
    //  handoff_complete: A tokio Notify that blocks us until the previous session's task is
    //    finished.  This means the previous session's WebSocket is closed, which guarantees no
    //    more communication from that client.
    let (map_updater, old_event_pipe, handoff_complete) = loop {
        match old_sink_ctx {
            Some(ctx) => {
                // There's another active session for this key.  Let's take it over.
                debug!("Handoff {:?} -> {:?} for {:?}", ctx.id, id, key);

                let packet = match ctx.handoff_session().await {
                    Ok(packet) => packet,
                    Err(e) => {
                        old_sink_ctx = None;
                        error!("Handoff {:?} -> {:?} cancelled: {}", ctx.id, id, e);

                        continue;
                    }
                };

                break (
                    packet.map_updater,
                    Some((ctx.event_sink, packet.event_stream)),
                    Some(packet.handoff_complete),
                );
            }
            None => {
                let mut unlocked_map = event_sink_map.write().await;
                match unlocked_map.entry(key) {
                    hash_map::Entry::Occupied(entry) => {
                        old_sink_ctx = entry.into_mut().get().await.ok();
                    }
                    hash_map::Entry::Vacant(entry) => {
                        info!("Starting session {:?} for {:?}", id, key);
                        let (map_updater, rx) = watch::channel(None);
                        entry.insert(EventSinkEntry::new(rx));
                        break (map_updater, None, None);
                    }
                }
            }
        }
    };

    if let Some(future) = handoff_complete {
        future.notified().await;
    }

    let event_pipe = old_event_pipe.unwrap_or_else(|| events::make_event_pipe(key));
    let event_sink_ctx = EventSinkCtx::new(id, session, event_pipe.0.clone());

    // We have everything we need to update the event sink map.  Broadcast it and block on
    // completion of the previous session.  If we can't broadcast, event sources won't be able to
    // publish events to our sink, so we need to bubble the error up to tear down our task.
    map_updater
        .broadcast(Some(event_sink_ctx))
        .map_err(|_| LifecycleError::MapUpdaterClosed)?;

    Ok((event_pipe, map_updater))
}

pub async fn run<T: Session>(mut session: T, ws: TcpWebSocketStream) -> anyhow::Result<()> {
    let (mut sink, stream) = ws.split();

    session.handle_open(&mut sink).await;

    let id = SessionId::next();
    let notify = Arc::new(Notify::new());
    let ctx = Ctx {
        id,
        session,
        notify,
        sink,
    };

    ctx.run(stream).await
}

impl<T: Session> Drop for Ctx<T> {
    fn drop(&mut self) {
        self.notify.notify();
        tokio::task::block_in_place(|| {
            futures::executor::block_on(self.session.handle_close(&mut self.sink))
        });
        info!(
            "Shut down session {:?} for {:?}",
            self.id,
            self.session.get_key()
        );
    }
}

impl<T: Session> Sink<Message> for Ctx<T> {
    type Error = WsError;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        let sink = unsafe { self.map_unchecked_mut(|s| &mut s.sink) };
        Sink::poll_ready(sink, cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        let sink = unsafe { self.map_unchecked_mut(|s| &mut s.sink) };
        Sink::start_send(sink, item)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        let sink = unsafe { self.map_unchecked_mut(|s| &mut s.sink) };
        Sink::poll_flush(sink, cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        let sink = unsafe { self.map_unchecked_mut(|s| &mut s.sink) };
        Sink::poll_close(sink, cx)
    }
}
