use std::any::Any;
use std::any::TypeId;
use std::collections::hash_map::Entry;
use std::collections::hash_map::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use futures::never::Never;
use futures::TryFutureExt;
use once_cell::sync::Lazy;
use tokio::sync::watch;
use tokio::sync::RwLock;

use super::events::EventSinkCtx;
use super::Session;

pub type SessionKindMap = RwLock<HashMap<TypeId, Arc<dyn Any + 'static + Sync + Send>>>;

pub struct EventSinkEntry<T: Session> {
    rx: watch::Receiver<Option<EventSinkCtx<T>>>,
}

impl<T: Session> Clone for EventSinkEntry<T> {
    fn clone(&self) -> Self {
        EventSinkEntry {
            rx: self.rx.clone(),
        }
    }
}

impl<T: Session> EventSinkEntry<T> {
    pub fn new(rx: watch::Receiver<Option<EventSinkCtx<T>>>) -> Self {
        EventSinkEntry { rx }
    }

    pub async fn get(&self) -> anyhow::Result<EventSinkCtx<T>> {
        let mut rx = self.rx.clone();
        loop {
            match rx.recv().await {
                Some(Some(ctx)) => break Ok(ctx),
                Some(None) => continue,
                None => break Err(anyhow!("Session closed")),
            }
        }
    }

    pub async fn try_get(&self) -> Option<EventSinkCtx<T>> {
        let mut rx = self.rx.clone();
        rx.recv().await.flatten()
    }
}

pub type EventSinkMap<T> = RwLock<HashMap<<T as Session>::Key, EventSinkEntry<T>>>;

async fn try_get_event_sink_map<T: Session>(
    map: &'static SessionKindMap,
) -> Result<Arc<EventSinkMap<T>>, ()> {
    let locked = map.read().await;
    locked
        .get(&TypeId::of::<T>())
        .map(|a| a.clone().downcast::<EventSinkMap<T>>().unwrap())
        .ok_or(())
}

async fn create_event_sink_map<T: Session>(
    map: &'static SessionKindMap,
) -> Result<Arc<EventSinkMap<T>>, Never> {
    let event_sink_map = {
        let mut locked = map.write().await;
        match locked.entry(TypeId::of::<T>()) {
            Entry::Occupied(entry) => entry.into_mut().clone(),
            Entry::Vacant(entry) => {
                let inner = <EventSinkMap<T> as Default>::default();
                entry.insert(Arc::new(inner)).clone()
            }
        }
    };

    event_sink_map
        .clone()
        .downcast::<EventSinkMap<T>>()
        .or_else(|_| unreachable!())
}

static SESSION_KIND_MAP: Lazy<SessionKindMap> = Lazy::new(Default::default);

pub async fn get_event_sink_map<T: Session>() -> Arc<EventSinkMap<T>> {
    try_get_event_sink_map(&SESSION_KIND_MAP).await.unwrap()
}

pub async fn get_or_create_event_sink_map<T: Session>() -> Arc<EventSinkMap<T>> {
    try_get_event_sink_map(&SESSION_KIND_MAP)
        .or_else(|_| create_event_sink_map(&SESSION_KIND_MAP))
        .await
        .unwrap()
}
