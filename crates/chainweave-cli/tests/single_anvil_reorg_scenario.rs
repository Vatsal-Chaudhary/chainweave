use std::{
    net::TcpListener,
    process::{Child, Command, Stdio},
    time::Duration,
};

use chainweave_core::{BlockHash, BlockHeader};
use chainweave_rpc::{ContractLogFilter, RpcClient};
use chainweave_sink::{IndexedBlock, OutboxEvent, PostgresChainWriter, ReconciliationError};
use serde_json::{Value, json};
use sqlx::Row;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use url::Url;

const ANVIL_VERSION: &str = "anvil Version: 1.7.1";
const ANVIL_COMMIT: &str = "Commit SHA: 4072e48705af9d93e3c0f6e29e93b5e9a40caed8";
const EVENT_TOPIC: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires pinned anvil 1.7.1 and a Postgres CHAINWEAVE_TEST_DATABASE_URL"]
async fn single_anvil_reorg_reconciles_postgres_state_and_outbox() {
    assert_pinned_anvil();
    let database_url =
        std::env::var("CHAINWEAVE_TEST_DATABASE_URL").expect("CHAINWEAVE_TEST_DATABASE_URL");
    let anvil_port = free_port();
    let http_url = Url::parse(&format!("http://127.0.0.1:{anvil_port}")).unwrap();
    let mut anvil = AnvilProcess::spawn(anvil_port);
    wait_for_anvil(&http_url).await;

    let client = RpcClient::connect(&http_url).await.unwrap();
    let head = client.head().await.unwrap();
    let writer = PostgresChainWriter::connect(&database_url, head.chain_id)
        .await
        .unwrap();
    writer.run_migrations().await.unwrap();
    writer
        .ensure_chain_identity(head.genesis_block_hash())
        .await
        .unwrap();

    let account = first_account(&http_url).await;
    let contract = deploy_log_contract(&http_url, &account).await;
    let filter = ContractLogFilter::address(&contract).unwrap();

    let deploy_block = client.fetch_header_by_number(1).await.unwrap();
    let old_second = send_contract_tx(&http_url, &account, &contract, "0x1111").await;
    let old_third = send_contract_tx(&http_url, &account, &contract, "0x2222").await;
    let old_second = receipt_header(&client, &old_second).await;
    let old_third = receipt_header(&client, &old_third).await;
    assert_eq!(old_second.height, 2);
    assert_eq!(old_third.height, 3);
    assert_eq!(old_second.parent_hash, deploy_block.hash);
    assert_eq!(old_third.parent_hash, old_second.hash);

    let mut old_source = AnvilReconciliationSource::new(client.clone(), filter);
    let old_summary = writer.reconcile_to_head(&mut old_source, 64).await.unwrap();
    assert_eq!(old_summary.applied_blocks, 4);
    assert_eq!(
        canonical_log_data(&writer).await,
        vec![bytes("1111"), bytes("2222")]
    );

    invoke_anvil_reorg(&http_url, &account, &contract).await;
    let new_second = client.fetch_header_by_number(2).await.unwrap();
    let new_third = client.fetch_header_by_number(3).await.unwrap();
    assert_ne!(old_second.hash, new_second.hash);
    assert_ne!(old_third.hash, new_third.hash);
    assert_eq!(new_second.parent_hash, deploy_block.hash);
    assert_eq!(new_third.parent_hash, new_second.hash);
    assert_eq!(
        anvil_log_data(&http_url, &contract).await,
        vec![bytes("aaaa"), bytes("bbbb")]
    );

    println!(
        "detection: common_ancestor_height=1 old_tip={} new_tip={}",
        hex_hash(&old_third.hash),
        hex_hash(&new_third.hash)
    );

    let mut new_source = AnvilReconciliationSource::new(client, filter);
    let reorg_summary = writer.reconcile_to_head(&mut new_source, 64).await.unwrap();
    assert_eq!(reorg_summary.rolled_back_blocks, 2);
    assert_eq!(reorg_summary.applied_blocks, 2);
    assert_eq!(
        reorg_summary.final_checkpoint.unwrap().last_hash,
        new_third.hash
    );
    assert_old_branch_retained_but_noncanonical(
        &writer,
        head.chain_id,
        [(old_second, bytes("1111")), (old_third, bytes("2222"))],
    )
    .await;

    let outbox = writer.outbox_events().await.unwrap();
    let reorg_outbox = &outbox[outbox.len() - 4..];
    assert_outbox(
        reorg_outbox,
        [
            ("rollback", old_third),
            ("rollback", old_second),
            ("apply", new_second),
            ("apply", new_third),
        ],
    );
    println!("rollback: heights=[3, 2]");
    println!("re-apply: heights=[2, 3]");

    let checkpoint = writer.checkpoint().await.unwrap().unwrap();
    assert_eq!(checkpoint.last_height, 3);
    assert_eq!(checkpoint.last_hash, new_third.hash);
    assert_eq!(
        writer.canonical_header_at_height(2).await.unwrap().unwrap(),
        new_second
    );
    assert_eq!(
        writer.canonical_header_at_height(3).await.unwrap().unwrap(),
        new_third
    );

    let final_logs = canonical_log_data(&writer).await;
    assert_eq!(final_logs, vec![bytes("aaaa"), bytes("bbbb")]);
    assert!(!final_logs.contains(&bytes("1111")));
    assert!(!final_logs.contains(&bytes("2222")));
    println!(
        "final query result: canonical_logs=[{}] checkpoint=3:{}",
        final_logs
            .iter()
            .map(|data| hex_bytes(data))
            .collect::<Vec<_>>()
            .join(", "),
        hex_hash(&checkpoint.last_hash)
    );

    anvil.kill();
}

async fn assert_old_branch_retained_but_noncanonical(
    writer: &PostgresChainWriter,
    chain_id: u64,
    old_branch: [(BlockHeader, Vec<u8>); 2],
) {
    let chain_id = chain_id.to_string();
    for (header, payload) in old_branch {
        let block_row = sqlx::query(
            r"
            SELECT is_canonical
            FROM blocks
            WHERE chain_id = ($1::text)::numeric
              AND block_hash = $2
              AND height = $3
            ",
        )
        .bind(&chain_id)
        .bind(header.hash.as_slice())
        .bind(i64::try_from(header.height).unwrap())
        .fetch_optional(writer.pool())
        .await
        .unwrap()
        .unwrap_or_else(|| {
            panic!(
                "old branch block {} was not retained",
                hex_hash(&header.hash)
            )
        });
        assert!(!block_row.get::<bool, _>("is_canonical"));

        let log_row = sqlx::query(
            r"
            SELECT data
            FROM logs
            WHERE chain_id = ($1::text)::numeric
              AND block_hash = $2
              AND log_index = 0
            ",
        )
        .bind(&chain_id)
        .bind(header.hash.as_slice())
        .fetch_optional(writer.pool())
        .await
        .unwrap()
        .unwrap_or_else(|| {
            panic!(
                "old branch log at block {}/log_index 0 was not retained",
                hex_hash(&header.hash)
            )
        });
        assert_eq!(log_row.get::<Vec<u8>, _>("data"), payload);
    }
}

#[derive(Debug)]
struct AnvilReconciliationSource {
    client: RpcClient,
    filter: ContractLogFilter,
}

impl AnvilReconciliationSource {
    const fn new(client: RpcClient, filter: ContractLogFilter) -> Self {
        Self { client, filter }
    }
}

impl chainweave_sink::ReconciliationSource for AnvilReconciliationSource {
    async fn head(&mut self) -> Result<IndexedBlock, ReconciliationError> {
        let head = self
            .client
            .head()
            .await
            .map_err(|error| ReconciliationError::Source(error.to_string()))?;
        fetch_indexed_block(&self.client, self.filter, head.number).await
    }

    async fn block_by_height(
        &mut self,
        height: u64,
    ) -> Result<Option<IndexedBlock>, ReconciliationError> {
        match fetch_indexed_block(&self.client, self.filter, height).await {
            Ok(block) => Ok(Some(block)),
            Err(ReconciliationError::MissingSourceHeight(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

async fn fetch_indexed_block(
    client: &RpcClient,
    filter: ContractLogFilter,
    height: u64,
) -> Result<IndexedBlock, ReconciliationError> {
    let mut block = client
        .fetch_block_by_number(height)
        .await
        .map_err(|error| {
            if error.to_string().contains("returned no block") {
                ReconciliationError::MissingSourceHeight(height)
            } else {
                ReconciliationError::Source(error.to_string())
            }
        })?;
    block.logs = client
        .fetch_logs_by_block_hash(block.header.hash, Some(filter))
        .await
        .map_err(|error| ReconciliationError::Source(error.to_string()))?;
    Ok(block)
}

struct AnvilProcess {
    child: Child,
}

impl AnvilProcess {
    fn spawn(port: u16) -> Self {
        let child = Command::new("anvil")
            .args([
                "--host",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--chain-id",
                "31337",
                "--quiet",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn anvil");
        Self { child }
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for AnvilProcess {
    fn drop(&mut self) {
        self.kill();
    }
}

fn assert_pinned_anvil() {
    let output = Command::new("anvil")
        .arg("--version")
        .output()
        .expect("anvil is required for the single Anvil reorg scenario");
    let stdout = String::from_utf8(output.stdout).expect("anvil --version returned non-UTF8");
    assert!(
        stdout.contains(ANVIL_VERSION) && stdout.contains(ANVIL_COMMIT),
        "single Anvil reorg scenario requires {ANVIL_VERSION} / {ANVIL_COMMIT}; got:\n{stdout}"
    );
}

async fn wait_for_anvil(http_url: &Url) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("anvil did not become ready");
        }
        if rpc_result(http_url, "eth_chainId", json!([])).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn first_account(http_url: &Url) -> String {
    rpc(http_url, "eth_accounts", json!([])).await[0]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn deploy_log_contract(http_url: &Url, from: &str) -> String {
    let tx_hash = rpc(
        http_url,
        "eth_sendTransaction",
        json!([{ "from": from, "data": event_contract_init_code() }]),
    )
    .await;
    let receipt = wait_for_receipt(http_url, tx_hash.as_str().unwrap()).await;
    receipt["contractAddress"].as_str().unwrap().to_owned()
}

async fn send_contract_tx(http_url: &Url, from: &str, contract: &str, data: &str) -> Value {
    let tx_hash = rpc(
        http_url,
        "eth_sendTransaction",
        json!([{ "from": from, "to": contract, "data": data }]),
    )
    .await;
    wait_for_receipt(http_url, tx_hash.as_str().unwrap()).await
}

async fn invoke_anvil_reorg(http_url: &Url, from: &str, contract: &str) {
    let result = rpc(
        http_url,
        "anvil_reorg",
        json!([
            2,
            [
                [{ "from": from, "to": contract, "input": "0xaaaa" }, 0],
                [{ "from": from, "to": contract, "input": "0xbbbb" }, 1],
            ]
        ]),
    )
    .await;
    assert!(
        result.is_null(),
        "anvil_reorg returned unexpected result: {result}"
    );
}

async fn receipt_header(client: &RpcClient, receipt: &Value) -> BlockHeader {
    let hash = parse_hash(receipt["blockHash"].as_str().unwrap());
    client.fetch_header_by_hash(hash).await.unwrap().unwrap()
}

async fn wait_for_receipt(http_url: &Url, tx_hash: &str) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("receipt did not become available for {tx_hash}");
        }
        let receipt = rpc(http_url, "eth_getTransactionReceipt", json!([tx_hash])).await;
        if !receipt.is_null() {
            return receipt;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn anvil_log_data(http_url: &Url, contract: &str) -> Vec<Vec<u8>> {
    rpc(
        http_url,
        "eth_getLogs",
        json!([{ "fromBlock": "0x0", "toBlock": "latest", "address": contract }]),
    )
    .await
    .as_array()
    .unwrap()
    .iter()
    .map(|log| bytes(log["data"].as_str().unwrap().trim_start_matches("0x")))
    .collect()
}

async fn canonical_log_data(writer: &PostgresChainWriter) -> Vec<Vec<u8>> {
    writer
        .canonical_logs()
        .await
        .unwrap()
        .into_iter()
        .map(|log| log.data)
        .collect()
}

fn assert_outbox<const N: usize>(events: &[OutboxEvent], expected: [(&str, BlockHeader); N]) {
    assert_eq!(events.len(), expected.len());
    for (event, (kind, header)) in events.iter().zip(expected) {
        assert_eq!(event.event_kind, kind);
        assert_eq!(event.block_height, header.height);
        assert_eq!(event.block_hash, header.hash);
    }
}

async fn rpc(http_url: &Url, method: &str, params: Value) -> Value {
    rpc_result(http_url, method, params).await.unwrap()
}

async fn rpc_result(http_url: &Url, method: &str, params: Value) -> Result<Value, String> {
    let host = http_url
        .host_str()
        .ok_or_else(|| "missing host".to_owned())?;
    let port = http_url.port_or_known_default().unwrap_or(80);
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    })
    .to_string();
    let mut stream = TcpStream::connect((host, port))
        .await
        .map_err(|error| error.to_string())?;
    let request = format!(
        "POST / HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|error| error.to_string())?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .map_err(|error| error.to_string())?;
    let response = String::from_utf8(response).map_err(|error| error.to_string())?;
    let (_, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("malformed HTTP response: {response}"))?;
    let value: Value = serde_json::from_str(body).map_err(|error| error.to_string())?;
    if !value["error"].is_null() {
        return Err(value["error"].to_string());
    }
    Ok(value["result"].clone())
}

fn event_contract_init_code() -> String {
    let runtime = format!(
        "3660006000377f{}366000a100",
        EVENT_TOPIC.trim_start_matches("0x")
    );
    format!("0x602c600c600039602c6000f3{runtime}")
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn parse_hash(value: &str) -> BlockHash {
    bytes32(value.trim_start_matches("0x"))
}

fn bytes32(hex: &str) -> BlockHash {
    let bytes = bytes(hex);
    bytes
        .try_into()
        .unwrap_or_else(|bytes: Vec<u8>| panic!("expected 32 bytes, got {}", bytes.len()))
}

fn bytes(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0);
    (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).unwrap())
        .collect()
}

fn hex_hash(hash: &BlockHash) -> String {
    hex_bytes(hash)
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut output = String::from("0x");
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}
