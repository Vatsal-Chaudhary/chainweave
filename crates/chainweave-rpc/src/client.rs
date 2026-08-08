use alloy::{
    primitives::B256,
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::types::BlockNumberOrTag,
};
use chainweave_core::ChainIdentity;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

#[derive(Debug, Clone)]
pub struct RpcClient {
    provider: DynProvider,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ChainHead {
    pub chain_id: u64,
    pub number: u64,
    pub hash: B256,
    pub genesis_hash: B256,
}

#[derive(Debug, Deserialize)]
struct BlockIdentity {
    hash: B256,
}

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("failed to connect to RPC endpoint: {0}")]
    Connect(String),
    #[error("RPC request failed: {0}")]
    Request(String),
    #[error("RPC returned no block for {0}")]
    MissingBlock(&'static str),
    #[error("configured chain ID {expected} does not match RPC chain ID {actual}")]
    ChainIdMismatch { expected: u64, actual: u64 },
    #[error("configured genesis hash {expected} does not match RPC genesis hash {actual}")]
    GenesisMismatch { expected: B256, actual: B256 },
    #[error("configured genesis hash is invalid: {0}")]
    InvalidGenesis(String),
}

impl RpcClient {
    /// Connects to an HTTP(S) or WS(S) Ethereum JSON-RPC endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError::Connect`] when Alloy cannot construct the selected transport.
    pub async fn connect(url: &Url) -> Result<Self, RpcError> {
        let provider = ProviderBuilder::new()
            .connect(url.as_str())
            .await
            .map_err(|error| RpcError::Connect(error.to_string()))?
            .erased();
        Ok(Self { provider })
    }

    /// Reads the current block number and the latest and genesis block hashes.
    ///
    /// # Errors
    ///
    /// Returns an error when an RPC request fails or either required block is missing.
    pub async fn head(&self) -> Result<ChainHead, RpcError> {
        let chain_id = self.provider.get_chain_id().await.map_err(request_error)?;
        let number = self
            .provider
            .get_block_number()
            .await
            .map_err(request_error)?;
        let latest: BlockIdentity = self
            .provider
            .raw_request::<_, Option<BlockIdentity>>(
                "eth_getBlockByNumber".into(),
                (BlockNumberOrTag::Latest, false),
            )
            .await
            .map_err(request_error)?
            .ok_or(RpcError::MissingBlock("latest"))?;
        let genesis: BlockIdentity = self
            .provider
            .raw_request::<_, Option<BlockIdentity>>(
                "eth_getBlockByNumber".into(),
                (BlockNumberOrTag::Number(0), false),
            )
            .await
            .map_err(request_error)?
            .ok_or(RpcError::MissingBlock("genesis"))?;

        Ok(ChainHead {
            chain_id,
            number,
            hash: latest.hash,
            genesis_hash: genesis.hash,
        })
    }

    /// Compares the observed chain identity with the configured expectation.
    ///
    /// # Errors
    ///
    /// Returns an error for a malformed expected hash or a chain ID/genesis mismatch.
    pub fn verify_identity(head: &ChainHead, expected: &ChainIdentity) -> Result<(), RpcError> {
        if head.chain_id != expected.chain_id {
            return Err(RpcError::ChainIdMismatch {
                expected: expected.chain_id,
                actual: head.chain_id,
            });
        }
        let expected_hash = expected
            .genesis_hash
            .parse::<B256>()
            .map_err(|error| RpcError::InvalidGenesis(error.to_string()))?;
        if head.genesis_hash != expected_hash {
            return Err(RpcError::GenesisMismatch {
                expected: expected_hash,
                actual: head.genesis_hash,
            });
        }
        Ok(())
    }
}

fn request_error(error: impl std::fmt::Display) -> RpcError {
    RpcError::Request(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use axum::{Json, Router, routing::post};
    use serde_json::{Value, json};
    use tokio::net::TcpListener;

    use super::*;

    const GENESIS_HASH: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HEAD_HASH: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    #[tokio::test]
    async fn reads_head_and_accepts_matching_identity() {
        let url = spawn_fixture_rpc().await;
        let client = RpcClient::connect(&url).await.unwrap();
        let head = client.head().await.unwrap();

        println!("fixture head: {}", serde_json::to_string(&head).unwrap());
        assert_eq!(head.chain_id, 31_337);
        assert_eq!(head.number, 2);
        assert_eq!(head.hash, HEAD_HASH.parse::<B256>().unwrap());
        RpcClient::verify_identity(
            &head,
            &ChainIdentity {
                chain_id: 31_337,
                genesis_hash: GENESIS_HASH.to_owned(),
            },
        )
        .unwrap();
    }

    #[tokio::test]
    async fn rejects_chain_id_and_genesis_mismatches() {
        let url = spawn_fixture_rpc().await;
        let head = RpcClient::connect(&url)
            .await
            .unwrap()
            .head()
            .await
            .unwrap();

        let chain_error = RpcClient::verify_identity(
            &head,
            &ChainIdentity {
                chain_id: 1,
                genesis_hash: GENESIS_HASH.to_owned(),
            },
        )
        .unwrap_err();
        assert!(matches!(chain_error, RpcError::ChainIdMismatch { .. }));

        let genesis_error = RpcClient::verify_identity(
            &head,
            &ChainIdentity {
                chain_id: 31_337,
                genesis_hash: "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                    .to_owned(),
            },
        )
        .unwrap_err();
        assert!(matches!(genesis_error, RpcError::GenesisMismatch { .. }));
    }

    async fn spawn_fixture_rpc() -> Url {
        let app = Router::new().route("/", post(fixture_rpc));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address: SocketAddr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Url::parse(&format!("http://{address}")).unwrap()
    }

    async fn fixture_rpc(Json(request): Json<Value>) -> Json<Value> {
        let method = request["method"].as_str().unwrap();
        let result = match method {
            "eth_chainId" => json!("0x7a69"),
            "eth_blockNumber" => json!("0x2"),
            "eth_getBlockByNumber" => {
                if request["params"][0] == "0x0" {
                    json!({ "hash": GENESIS_HASH })
                } else {
                    json!({ "hash": HEAD_HASH })
                }
            }
            _ => panic!("unexpected fixture RPC method: {method}"),
        };
        Json(json!({
            "jsonrpc": "2.0",
            "id": request["id"],
            "result": result,
        }))
    }
}
