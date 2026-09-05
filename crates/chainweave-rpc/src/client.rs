use alloy::{
    primitives::{Address, B256, Bytes},
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::types::BlockNumberOrTag,
};
use chainweave_core::{BackfillRange, BlockHeader, ChainIdentity, FetchedRange};
use chainweave_sink::{BlockStatus, IndexedBlock, RawLog, StatusSource};
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::json;
use thiserror::Error;
use time::OffsetDateTime;
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

impl ChainHead {
    #[must_use]
    pub const fn block_hash(&self) -> [u8; 32] {
        self.hash.0
    }

    #[must_use]
    pub const fn genesis_block_hash(&self) -> [u8; 32] {
        self.genesis_hash.0
    }
}

#[derive(Debug, Deserialize)]
struct BlockIdentity {
    hash: B256,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcBlock {
    hash: B256,
    parent_hash: B256,
    #[serde(deserialize_with = "deserialize_quantity")]
    number: u64,
    #[serde(deserialize_with = "deserialize_quantity")]
    timestamp: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcLog {
    block_hash: Option<B256>,
    #[serde(default, deserialize_with = "deserialize_optional_quantity")]
    block_number: Option<u64>,
    transaction_hash: Option<B256>,
    #[serde(default, deserialize_with = "deserialize_optional_quantity")]
    transaction_index: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_optional_quantity")]
    log_index: Option<u64>,
    address: Address,
    #[serde(default)]
    topics: Vec<B256>,
    data: Bytes,
    #[serde(default)]
    removed: bool,
}

#[derive(Debug, Clone)]
struct AnchoredRawLog {
    block_hash: [u8; 32],
    raw: RawLog,
}

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("failed to connect to RPC endpoint: {0}")]
    Connect(String),
    #[error("RPC request failed: {0}")]
    Request(String),
    #[error("RPC returned no block for {0}")]
    MissingBlock(&'static str),
    #[error("RPC returned no block at height {0}")]
    MissingBlockNumber(u64),
    #[error("RPC block {0} is missing a required field: {1}")]
    MissingBlockField(u64, &'static str),
    #[error("RPC log is missing a required field: {0}")]
    MissingLogField(&'static str),
    #[error("RPC log belongs to block {actual}, outside requested range {from_block}..={to_block}")]
    LogOutsideRange {
        actual: u64,
        from_block: u64,
        to_block: u64,
    },
    #[error("RPC log block hash does not match fetched header at height {height}")]
    LogBlockHashMismatch { height: u64 },
    #[error("RPC log integer field {field} value {value} exceeds supported range")]
    LogIntegerOverflow { field: &'static str, value: u64 },
    #[error("RPC block timestamp {0} is outside supported range")]
    InvalidTimestamp(u64),
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

    /// Captures the target canonical head before a bounded backfill starts.
    ///
    /// # Errors
    ///
    /// Returns an error when the RPC cannot provide the current head and genesis identity.
    pub async fn capture_target_head(&self) -> Result<ChainHead, RpcError> {
        self.head().await
    }

    /// Fetches a block header by canonical block number.
    ///
    /// # Errors
    ///
    /// Returns an error when the RPC request fails or the block/header response is incomplete.
    pub async fn fetch_header_by_number(&self, height: u64) -> Result<BlockHeader, RpcError> {
        let block = self.fetch_rpc_block_by_number(height).await?;
        block_header(&block)
    }

    /// Fetches a block by canonical block number and converts it to durable sink input.
    ///
    /// The returned block has no logs attached; range backfill attaches logs from
    /// [`Self::fetch_logs`] so the log query can be sized and retried independently.
    ///
    /// # Errors
    ///
    /// Returns an error when the RPC request fails or the block/header response is incomplete.
    pub async fn fetch_block_by_number(&self, height: u64) -> Result<IndexedBlock, RpcError> {
        let block = self.fetch_rpc_block_by_number(height).await?;
        indexed_block_from_rpc(block, Vec::new())
    }

    /// Fetches logs for an inclusive canonical block range.
    ///
    /// # Errors
    ///
    /// Returns an error when the RPC request fails or a log lacks the identity fields required for
    /// idempotent durable writes.
    pub async fn fetch_logs(&self, range: BackfillRange) -> Result<Vec<RawLog>, RpcError> {
        self.fetch_rpc_logs(range)
            .await?
            .into_iter()
            .map(|log| raw_log_from_rpc(log, range))
            .collect::<Result<Vec<_>, _>>()
    }

    /// Fetches headers and logs for one bounded backfill range.
    ///
    /// # Errors
    ///
    /// Returns an error if any block or log request fails, or if fetched headers are not continuous.
    pub async fn fetch_backfill_range(
        &self,
        range: BackfillRange,
    ) -> Result<FetchedRange<IndexedBlock>, RpcError> {
        let mut logs_by_height = logs_by_height(self.fetch_rpc_logs(range).await?, range)?;
        let mut blocks = Vec::with_capacity(range.len() as usize);
        let mut headers = Vec::with_capacity(range.len() as usize);
        for height in range.from_block..=range.to_block {
            let block = self.fetch_rpc_block_by_number(height).await?;
            let header = block_header(&block)?;
            headers.push(header);
            let logs = logs_by_height
                .remove(&height)
                .unwrap_or_default()
                .into_iter()
                .map(|log| {
                    if log.block_hash != header.hash {
                        return Err(RpcError::LogBlockHashMismatch { height });
                    }
                    Ok(log.raw)
                })
                .collect::<Result<Vec<_>, _>>()?;
            blocks.push(indexed_block_from_rpc(block, logs)?);
        }
        FetchedRange::new(range, headers, blocks)
            .map_err(|error| RpcError::Request(error.to_string()))
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

impl RpcClient {
    async fn fetch_rpc_block_by_number(&self, height: u64) -> Result<RpcBlock, RpcError> {
        self.provider
            .raw_request::<_, Option<RpcBlock>>(
                "eth_getBlockByNumber".into(),
                (BlockNumberOrTag::Number(height), false),
            )
            .await
            .map_err(request_error)?
            .ok_or(RpcError::MissingBlockNumber(height))
    }

    async fn fetch_rpc_logs(&self, range: BackfillRange) -> Result<Vec<RpcLog>, RpcError> {
        let filter = json!({
            "fromBlock": format_quantity(range.from_block),
            "toBlock": format_quantity(range.to_block),
        });
        self.provider
            .raw_request::<_, Vec<RpcLog>>("eth_getLogs".into(), (filter,))
            .await
            .map_err(request_error)
    }
}

fn indexed_block_from_rpc(block: RpcBlock, logs: Vec<RawLog>) -> Result<IndexedBlock, RpcError> {
    let header = block_header(&block)?;
    let timestamp = i64::try_from(block.timestamp)
        .map_err(|_| RpcError::InvalidTimestamp(block.timestamp))
        .and_then(|timestamp| {
            OffsetDateTime::from_unix_timestamp(timestamp)
                .map_err(|_| RpcError::InvalidTimestamp(block.timestamp))
        })?;
    Ok(IndexedBlock {
        header,
        timestamp,
        status: BlockStatus::Unsafe,
        status_source: StatusSource::Observed,
        logs,
    })
}

fn block_header(block: &RpcBlock) -> Result<BlockHeader, RpcError> {
    Ok(BlockHeader::new(
        fixed_32(block.hash),
        fixed_32(block.parent_hash),
        block.number,
    ))
}

fn raw_log_from_rpc(log: RpcLog, range: BackfillRange) -> Result<RawLog, RpcError> {
    anchored_raw_log_from_rpc(log, range).map(|log| log.raw)
}

fn anchored_raw_log_from_rpc(
    log: RpcLog,
    range: BackfillRange,
) -> Result<AnchoredRawLog, RpcError> {
    if log.removed {
        return Err(RpcError::Request(
            "eth_getLogs returned a removed log for a historical range".to_owned(),
        ));
    }
    let block_number = log
        .block_number
        .ok_or(RpcError::MissingLogField("block_number"))?;
    if !range.contains(block_number) {
        return Err(RpcError::LogOutsideRange {
            actual: block_number,
            from_block: range.from_block,
            to_block: range.to_block,
        });
    }
    let transaction_index = required_u32(log.transaction_index, "transaction_index")?;
    let log_index = required_u32(log.log_index, "log_index")?;
    let block_hash = log
        .block_hash
        .map(fixed_32)
        .ok_or(RpcError::MissingLogField("block_hash"))?;
    let tx_hash = log
        .transaction_hash
        .map(fixed_32)
        .ok_or(RpcError::MissingLogField("transaction_hash"))?;
    let address = fixed_20(log.address);
    let topics = log.topics.into_iter().map(fixed_32).collect();
    let data = log.data.as_ref().to_vec();

    Ok(AnchoredRawLog {
        block_hash,
        raw: RawLog {
            transaction_index,
            log_index,
            tx_hash,
            address,
            topics,
            data,
            decoded_event: None,
            decoder_version: None,
        },
    })
}

fn logs_by_height(
    logs: Vec<RpcLog>,
    range: BackfillRange,
) -> Result<std::collections::BTreeMap<u64, Vec<AnchoredRawLog>>, RpcError> {
    let mut grouped = std::collections::BTreeMap::new();
    for log in logs {
        let block_number = log
            .block_number
            .ok_or(RpcError::MissingLogField("block_number"))?;
        if !range.contains(block_number) {
            return Err(RpcError::LogOutsideRange {
                actual: block_number,
                from_block: range.from_block,
                to_block: range.to_block,
            });
        }
        let raw = anchored_raw_log_from_rpc(log, range)?;
        grouped
            .entry(block_number)
            .or_insert_with(Vec::new)
            .push(raw);
    }
    for logs in grouped.values_mut() {
        logs.sort_by_key(|log| (log.raw.transaction_index, log.raw.log_index));
    }
    Ok(grouped)
}

fn required_u32(value: Option<u64>, field: &'static str) -> Result<u32, RpcError> {
    let value = value.ok_or(RpcError::MissingLogField(field))?;
    u32::try_from(value).map_err(|_| RpcError::LogIntegerOverflow { field, value })
}

fn fixed_32(value: B256) -> [u8; 32] {
    value.0
}

fn fixed_20(value: alloy::primitives::Address) -> [u8; 20] {
    let mut bytes = [0_u8; 20];
    bytes.copy_from_slice(value.as_ref());
    bytes
}

fn deserialize_quantity<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    parse_quantity(&value).map_err(de::Error::custom)
}

fn deserialize_optional_quantity<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let Some(value) = Option::<serde_json::Value>::deserialize(deserializer)? else {
        return Ok(None);
    };
    parse_quantity(&value).map(Some).map_err(de::Error::custom)
}

fn parse_quantity(value: &serde_json::Value) -> Result<u64, String> {
    if let Some(number) = value.as_u64() {
        return Ok(number);
    }
    let Some(hex) = value.as_str() else {
        return Err("quantity must be a hex string or unsigned integer".to_owned());
    };
    let digits = hex.strip_prefix("0x").unwrap_or(hex);
    u64::from_str_radix(digits, 16).map_err(|error| error.to_string())
}

fn format_quantity(value: u64) -> String {
    format!("0x{value:x}")
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

    #[tokio::test]
    async fn fetches_block_logs_and_ordered_backfill_range_from_fixture() {
        let url = spawn_fixture_rpc().await;
        let client = RpcClient::connect(&url).await.unwrap();
        let range = BackfillRange::new(1, 2).unwrap();

        let header = client.fetch_header_by_number(1).await.unwrap();
        assert_eq!(header.height, 1);
        assert_eq!(header.hash, hash(0xcc));
        assert_eq!(header.parent_hash, hash(0xaa));

        let logs = client.fetch_logs(range).await.unwrap();
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].transaction_index, 0);
        assert_eq!(logs[0].log_index, 0);
        assert_eq!(logs[0].tx_hash, hash(0x11));
        assert_eq!(logs[0].address, [0x22; 20]);
        assert_eq!(logs[0].topics, vec![hash(0x33)]);
        assert_eq!(logs[0].data, vec![0x44, 0x55]);

        let fetched = client.fetch_backfill_range(range).await.unwrap();
        assert_eq!(fetched.headers.len(), 2);
        assert_eq!(fetched.headers[0].height, 1);
        assert_eq!(fetched.headers[1].height, 2);
        assert_eq!(fetched.headers[1].parent_hash, fetched.headers[0].hash);
        assert_eq!(fetched.logs.len(), 2);
        assert_eq!(fetched.logs[0].logs.len(), 1);
        assert_eq!(fetched.logs[1].logs.len(), 1);
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
                let height = request["params"][0].as_str().unwrap();
                block_response(height)
            }
            "eth_getLogs" => {
                let filter = &request["params"][0];
                assert_eq!(filter["fromBlock"], "0x1");
                assert_eq!(filter["toBlock"], "0x2");
                json!([
                    log_response(
                        "0x1",
                        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                        "0x0",
                        "0x0",
                        "0x11",
                        "0x22",
                        "0x33",
                        "0x4455"
                    ),
                    log_response(
                        "0x2", HEAD_HASH, "0x0", "0x1", "0x12", "0x23", "0x34", "0x66"
                    )
                ])
            }
            _ => panic!("unexpected fixture RPC method: {method}"),
        };
        Json(json!({
            "jsonrpc": "2.0",
            "id": request["id"],
            "result": result,
        }))
    }

    fn block_response(height: &str) -> Value {
        match height {
            "0x0" => block_json("0x0", GENESIS_HASH, ZERO_HASH, "0x65"),
            "0x1" => block_json(
                "0x1",
                "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                GENESIS_HASH,
                "0x66",
            ),
            "0x2" | "latest" => block_json(
                "0x2",
                HEAD_HASH,
                "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "0x67",
            ),
            other => panic!("unexpected fixture block height {other}"),
        }
    }

    fn block_json(number: &str, hash: &str, parent_hash: &str, timestamp: &str) -> Value {
        json!({
            "number": number,
            "hash": hash,
            "parentHash": parent_hash,
            "timestamp": timestamp,
        })
    }

    fn log_response(
        block_number: &str,
        block_hash: &str,
        transaction_index: &str,
        log_index: &str,
        tx_hash_byte: &str,
        address_byte: &str,
        topic_byte: &str,
        data: &str,
    ) -> Value {
        json!({
            "blockNumber": block_number,
            "blockHash": block_hash,
            "transactionIndex": transaction_index,
            "logIndex": log_index,
            "transactionHash": repeat_hash(tx_hash_byte),
            "address": repeat_address(address_byte),
            "topics": [repeat_hash(topic_byte)],
            "data": data,
            "removed": false,
        })
    }

    fn repeat_hash(byte: &str) -> String {
        let byte = byte.strip_prefix("0x").unwrap();
        format!("0x{}", byte.repeat(32))
    }

    fn repeat_address(byte: &str) -> String {
        let byte = byte.strip_prefix("0x").unwrap();
        format!("0x{}", byte.repeat(20))
    }

    fn hash(value: u8) -> [u8; 32] {
        [value; 32]
    }

    const ZERO_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";
}
