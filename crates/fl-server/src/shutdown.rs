//! A one-way shutdown signal that any number of tasks can wait on.

use std::sync::Arc;

use tokio::sync::watch;

/// A cloneable handle to the node's shutdown signal.
///
/// Once triggered it stays triggered. Waiting on an already-triggered
/// signal returns immediately, so a task that starts late cannot miss it.
#[derive(Clone)]
pub struct Shutdown {
    tx: Arc<watch::Sender<bool>>,
}

impl Default for Shutdown {
    fn default() -> Self {
        Shutdown::new()
    }
}

impl Shutdown {
    /// A signal that has not been triggered.
    pub fn new() -> Self {
        let (tx, _rx) = watch::channel(false);
        Shutdown { tx: Arc::new(tx) }
    }

    /// Starts the shutdown. Safe to call more than once and from any
    /// thread.
    pub fn trigger(&self) {
        self.tx.send_replace(true);
    }

    /// True once [`Shutdown::trigger`] has been called.
    pub fn is_triggered(&self) -> bool {
        *self.tx.borrow()
    }

    /// Completes when the shutdown has been triggered.
    pub async fn wait(&self) {
        let mut rx = self.tx.subscribe();
        // The sender lives in `self`, so the channel cannot close while we
        // wait and the error case cannot happen.
        let _ = rx.wait_for(|triggered| *triggered).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn waiters_wake_when_triggered() {
        let shutdown = Shutdown::new();
        assert!(!shutdown.is_triggered());

        let waiter = {
            let shutdown = shutdown.clone();
            tokio::spawn(async move { shutdown.wait().await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        shutdown.trigger();
        waiter.await.unwrap();
        assert!(shutdown.is_triggered());
    }

    #[tokio::test]
    async fn late_waiters_return_immediately() {
        let shutdown = Shutdown::new();
        shutdown.trigger();
        shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(1), shutdown.wait())
            .await
            .expect("wait should not block after trigger");
    }
}
