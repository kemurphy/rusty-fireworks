use std::fmt;

#[derive(Copy, Clone, PartialOrd, Ord, PartialEq, Eq, fmt::Debug)]
pub struct SessionId(i64);

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Session:{}", self.0)
    }
}

impl SessionId {
    pub fn next() -> SessionId {
        use std::sync::atomic::{AtomicI64, Ordering};
        static SESSION_COUNTER: AtomicI64 = AtomicI64::new(0);
        SessionId(SESSION_COUNTER.fetch_add(1, Ordering::SeqCst))
    }
}
