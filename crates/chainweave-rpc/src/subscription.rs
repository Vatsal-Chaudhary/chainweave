use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use futures::StreamExt;
use tokio::{
    sync::mpsc::{self, Receiver, Sender, error::TrySendError},
    task::JoinHandle,
};
use url::Url;

use crate::RpcError;

#[derive(Debug, Clone)]
pub struct NewHeadWakeupSender {
    inner: Sender<()>,
}

impl NewHeadWakeupSender {
    #[must_use]
    pub const fn new(inner: Sender<()>) -> Self {
        Self { inner }
    }

    /// Queues one reconciliation wakeup, coalescing if an unread wakeup is already pending.
    ///
    /// Returns true when this call queued a new wakeup and false when the capacity-1 channel was
    /// already full or the receiver has closed.
    pub fn try_wake(&self) -> bool {
        match self.inner.try_send(()) {
            Ok(()) => true,
            Err(TrySendError::Full(())) | Err(TrySendError::Closed(())) => false,
        }
    }
}

#[must_use]
pub fn new_head_wakeup_channel() -> (NewHeadWakeupSender, Receiver<()>) {
    let (sender, receiver) = mpsc::channel(1);
    (NewHeadWakeupSender::new(sender), receiver)
}

/// Spawns a WebSocket `newHeads` subscription task that treats headers as reconciliation wakeups.
///
/// The channel intentionally has capacity 1 and drops/coalesces extra wakeups while one is already
/// pending. Correctness comes from explicit reconciliation after each wakeup or poll interval, not
/// from receiving every notification.
#[must_use]
pub fn spawn_new_heads_wakeup(
    ws_url: Url,
    wakeups: NewHeadWakeupSender,
) -> JoinHandle<Result<(), RpcError>> {
    tokio::spawn(async move {
        let provider = ProviderBuilder::new()
            .connect_ws(WsConnect::new(ws_url.to_string()))
            .await
            .map_err(|error| RpcError::Connect(error.to_string()))?;
        let subscription = provider
            .subscribe_blocks()
            .await
            .map_err(|error| RpcError::Request(error.to_string()))?;
        let mut stream = subscription.into_stream();

        while stream.next().await.is_some() {
            let _ = wakeups.try_wake();
        }

        Err(RpcError::Request(
            "newHeads subscription ended before shutdown".to_owned(),
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn new_head_wakeup_channel_coalesces_to_one_pending_item() {
        let (wakeups, mut receiver) = new_head_wakeup_channel();

        assert!(wakeups.try_wake());
        assert!(!wakeups.try_wake());
        assert_eq!(receiver.try_recv(), Ok(()));
        assert!(wakeups.try_wake());
        assert_eq!(receiver.try_recv(), Ok(()));
    }
}
