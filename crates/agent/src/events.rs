//! A small ring of recent agent-side problems, carried home on the heartbeat.
//!
//! Command failures already reach the controller inside a `Response`. This is
//! for everything else — the errors that would otherwise only exist in the
//! agent's own journal, on a machine nobody is watching:
//!
//! * a heartbeat that failed to upload,
//! * a poll that could not reach S3,
//! * a response lost after the command already ran.
//!
//! The buffer is deliberately tiny and lossy. It is a signal that something is
//! wrong and roughly what, not an audit trail; the agent journal remains the
//! authority.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use common::protocol::{now_unix, AgentEvent, EventKind};

/// Most events kept. The heartbeat object stays small enough to ignore.
const CAPACITY: usize = 20;
/// Longest single message retained. An S3 SDK error can run to several
/// kilobytes of nested context, and twenty of those would bloat every
/// heartbeat for no extra insight.
const MAX_MESSAGE_BYTES: usize = 400;

#[derive(Clone, Default)]
pub struct EventLog {
    inner: Arc<Mutex<VecDeque<AgentEvent>>>,
}

impl EventLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, kind: EventKind, message: impl Into<String>) {
        let event = AgentEvent {
            at: now_unix(),
            kind,
            message: truncate(message.into()),
        };
        // A poisoned mutex here must not take down the agent: losing telemetry
        // is strictly better than losing the command loop.
        let Ok(mut events) = self.inner.lock() else { return };
        if events.len() == CAPACITY {
            events.pop_front();
        }
        events.push_back(event);
    }

    /// Snapshot for the next heartbeat. Events are kept, not consumed, so a
    /// controller that checks every few minutes still sees what happened.
    pub fn snapshot(&self) -> Vec<AgentEvent> {
        self.inner
            .lock()
            .map(|events| events.iter().cloned().collect())
            .unwrap_or_default()
    }
}

fn truncate(mut message: String) -> String {
    if message.len() <= MAX_MESSAGE_BYTES {
        return message;
    }
    // Cut on a char boundary so the result stays valid UTF-8.
    let mut end = MAX_MESSAGE_BYTES;
    while end > 0 && !message.is_char_boundary(end) {
        end -= 1;
    }
    message.truncate(end);
    message.push('…');
    message
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_only_the_most_recent_events() {
        let log = EventLog::new();
        for index in 0..CAPACITY + 5 {
            log.record(EventKind::Poll, format!("event {index}"));
        }
        let events = log.snapshot();
        assert_eq!(events.len(), CAPACITY);
        assert_eq!(events[0].message, "event 5");
        assert_eq!(events[CAPACITY - 1].message, format!("event {}", CAPACITY + 4));
    }

    #[test]
    fn snapshot_does_not_consume() {
        let log = EventLog::new();
        log.record(EventKind::Heartbeat, "boom");
        assert_eq!(log.snapshot().len(), 1);
        assert_eq!(log.snapshot().len(), 1);
    }

    #[test]
    fn truncates_on_a_char_boundary() {
        let log = EventLog::new();
        // Multi-byte characters straddling the cut must not produce invalid UTF-8.
        log.record(EventKind::Poll, "错".repeat(500));
        let events = log.snapshot();
        assert!(events[0].message.len() <= MAX_MESSAGE_BYTES + 4);
        assert!(events[0].message.ends_with('…'));
    }
}
