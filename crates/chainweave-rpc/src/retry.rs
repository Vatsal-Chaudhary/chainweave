use std::{future::Future, time::Duration};

use chainweave_core::{BlockHeader, RetryDecision, RetryPolicy, RpcFailure};

use crate::{ChainHead, RpcClient, RpcError};

/// Classifies JSON-RPC errors into the shared retry vocabulary.
#[must_use]
pub fn classify_rpc_error(error: &RpcError) -> RpcFailure {
    match error {
        RpcError::Request(message) => {
            if is_hash_anchored_log_block_not_found(message) {
                return RpcFailure::Permanent;
            }
            let classified = RpcFailure::from_rpc_message(message);
            if matches!(classified, RpcFailure::Permanent) {
                RpcFailure::Transient
            } else {
                classified
            }
        }
        _ => RpcFailure::Permanent,
    }
}

fn is_hash_anchored_log_block_not_found(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("eth_getlogs")
        && (lower.contains("blockhash") || lower.contains("block hash"))
        && lower.contains("block not found")
}

/// Runs one RPC operation with the shared bounded retry and timeout policy.
///
/// # Errors
///
/// Returns the final RPC error once the policy gives up, or a timeout-shaped RPC error when the
/// request never completes within the configured timeout and retry attempts.
pub async fn retry_rpc_request<T, F, Fut>(
    retry_policy: RetryPolicy,
    rpc_timeout: Duration,
    timeout_error: impl Fn() -> RpcError,
    mut operation: F,
) -> Result<T, RpcError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, RpcError>>,
{
    let mut attempt = 1;
    loop {
        match tokio::time::timeout(rpc_timeout, operation()).await {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(error)) => match retry_policy.decision(classify_rpc_error(&error), attempt) {
                RetryDecision::RetryAfter(delay) => {
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                RetryDecision::GiveUp => return Err(error),
            },
            Err(_) => match retry_policy.decision(RpcFailure::Timeout, attempt) {
                RetryDecision::RetryAfter(delay) => {
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                RetryDecision::GiveUp => return Err(timeout_error()),
            },
        }
    }
}

/// Captures the current target head through the shared retry policy.
///
/// # Errors
///
/// Returns an RPC error when all retry attempts fail or time out.
pub async fn capture_target_head_with_retry(
    client: &RpcClient,
    retry_policy: RetryPolicy,
    rpc_timeout: Duration,
) -> Result<ChainHead, RpcError> {
    retry_rpc_request(
        retry_policy,
        rpc_timeout,
        || RpcError::Request("timed out while capturing target head".to_owned()),
        || client.capture_target_head(),
    )
    .await
}

/// Fetches one header by number through the shared retry policy.
///
/// # Errors
///
/// Returns an RPC error when all retry attempts fail or time out.
pub async fn fetch_header_by_number_with_retry(
    client: &RpcClient,
    height: u64,
    retry_policy: RetryPolicy,
    rpc_timeout: Duration,
) -> Result<BlockHeader, RpcError> {
    retry_rpc_request(
        retry_policy,
        rpc_timeout,
        || RpcError::Request(format!("timed out fetching header at height {height}")),
        || client.fetch_header_by_number(height),
    )
    .await
}

/// Fetches one header by hash through the shared retry policy.
///
/// # Errors
///
/// Returns an RPC error when all retry attempts fail or time out.
pub async fn fetch_header_by_hash_with_retry(
    client: &RpcClient,
    hash: [u8; 32],
    retry_policy: RetryPolicy,
    rpc_timeout: Duration,
) -> Result<Option<BlockHeader>, RpcError> {
    retry_rpc_request(
        retry_policy,
        rpc_timeout,
        || RpcError::Request(format!("timed out fetching header by hash {hash:?}")),
        || client.fetch_header_by_hash(hash),
    )
    .await
}

#[cfg(test)]
mod tests {
    use chainweave_core::RpcFailure;

    use super::{RpcError, classify_rpc_error};

    #[test]
    fn hash_anchored_log_block_not_found_is_not_retried() {
        let error =
            RpcError::Request("eth_getLogs blockHash 0xabc failed: block not found".to_owned());

        assert_eq!(classify_rpc_error(&error), RpcFailure::Permanent);
    }
}
