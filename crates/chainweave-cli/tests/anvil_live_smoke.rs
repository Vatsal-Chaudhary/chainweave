use std::{
    fs,
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chainweave_rpc::RpcClient;
use chainweave_sink::PostgresChainWriter;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use url::Url;

const CONTRACT: &str = "0x1000000000000000000000000000000000000001";
const TOPIC: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires anvil, a Postgres DATABASE_URL, and process kill support"]
async fn anvil_live_runner_reorg_and_kill_restart_smoke() {
    let database_url =
        std::env::var("CHAINWEAVE_TEST_DATABASE_URL").expect("CHAINWEAVE_TEST_DATABASE_URL");
    let anvil_port = free_port();
    let http_url = Url::parse(&format!("http://127.0.0.1:{anvil_port}")).unwrap();
    let ws_url = Url::parse(&format!("ws://127.0.0.1:{anvil_port}")).unwrap();
    let mut anvil = AnvilProcess::spawn(anvil_port);
    wait_for_anvil(&http_url).await;

    let client = RpcClient::connect(&http_url).await.unwrap();
    let head = client.head().await.unwrap();
    rpc(
        &http_url,
        "anvil_setCode",
        json!([CONTRACT, event_runtime_code()]),
    )
    .await;
    let account = rpc(&http_url, "eth_accounts", json!([])).await[0]
        .as_str()
        .unwrap()
        .to_owned();

    let workspace = TestWorkspace::new();
    let config_path = workspace.config_path(
        &database_url,
        &ws_url,
        head.chain_id,
        &head.genesis_hash.to_string(),
    );
    let mut runner = RunnerProcess::spawn(&config_path);
    let writer = PostgresChainWriter::connect(&database_url, head.chain_id)
        .await
        .unwrap();
    wait_for_checkpoint(&writer, 0, None).await;

    let first_receipt = send_log_tx(&http_url, &account).await;
    let first_hash = parse_hash(first_receipt["blockHash"].as_str().unwrap());
    assert!(
        client
            .fetch_header_by_hash(first_hash)
            .await
            .unwrap()
            .is_some()
    );
    let first_logs = rpc(
        &http_url,
        "eth_getLogs",
        json!([{ "blockHash": first_receipt["blockHash"], "address": CONTRACT }]),
    )
    .await;
    assert_eq!(first_logs.as_array().unwrap().len(), 1);
    wait_for_checkpoint(&writer, 1, Some(first_hash)).await;

    let snapshot = rpc(&http_url, "anvil_snapshot", json!([])).await;
    let old_second = send_log_tx(&http_url, &account).await;
    let old_second_hash = parse_hash(old_second["blockHash"].as_str().unwrap());
    wait_for_checkpoint(&writer, 2, Some(old_second_hash)).await;

    assert_eq!(
        rpc(&http_url, "anvil_revert", json!([snapshot]))
            .await
            .as_bool(),
        Some(true)
    );
    let new_second = send_log_tx(&http_url, &account).await;
    let new_second_hash = parse_hash(new_second["blockHash"].as_str().unwrap());
    assert_ne!(old_second_hash, new_second_hash);
    wait_for_checkpoint(&writer, 2, Some(new_second_hash)).await;
    assert_db_matches_anvil(&writer, &http_url).await;

    let third = send_log_tx(&http_url, &account).await;
    let third_hash = parse_hash(third["blockHash"].as_str().unwrap());
    runner.kill();
    runner = RunnerProcess::spawn(&config_path);
    wait_for_checkpoint(&writer, 3, Some(third_hash)).await;
    assert_db_matches_anvil(&writer, &http_url).await;

    runner.kill();
    anvil.kill();
}

struct TestWorkspace {
    path: PathBuf,
}

impl TestWorkspace {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "chainweave-anvil-smoke-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    fn config_path(
        &self,
        database_url: &str,
        ws_url: &Url,
        chain_id: u64,
        genesis_hash: &str,
    ) -> PathBuf {
        let path = self.path.join("chainweave.toml");
        fs::write(
            &path,
            format!(
                r#"
database_url = "{database_url}"

[rpc]
primary_url = "{ws_url}"

[indexer]
header_cache_size = 16
max_reorg_depth = 64

[server]
listen_addr = "127.0.0.1:0"

[expected_chain]
chain_id = {chain_id}
genesis_hash = "{genesis_hash}"
"#
            ),
        )
        .unwrap();
        path
    }
}

impl Drop for TestWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
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

struct RunnerProcess {
    child: Child,
}

impl RunnerProcess {
    fn spawn(config_path: &PathBuf) -> Self {
        let binary = env!("CARGO_BIN_EXE_chainweave");
        let child = Command::new(binary)
            .args([
                "--config",
                config_path.to_str().unwrap(),
                "live",
                "--start-block",
                "0",
                "--poll-interval-ms",
                "60000",
                "--rpc-timeout-ms",
                "5000",
                "--budget-cost-units",
                "10000",
                "--shutdown-timeout-ms",
                "30000",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn chainweave live runner");
        Self { child }
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for RunnerProcess {
    fn drop(&mut self) {
        self.kill();
    }
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

async fn send_log_tx(http_url: &Url, from: &str) -> Value {
    let tx_hash = rpc(
        http_url,
        "eth_sendTransaction",
        json!([{ "from": from, "to": CONTRACT, "data": "0x" }]),
    )
    .await;
    wait_for_receipt(http_url, tx_hash.as_str().unwrap()).await
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

async fn wait_for_checkpoint(writer: &PostgresChainWriter, height: u64, hash: Option<[u8; 32]>) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("checkpoint did not reach height {height}");
        }
        if let Ok(Some(checkpoint)) = writer.checkpoint().await {
            if checkpoint.last_height == height
                && hash.is_none_or(|hash| checkpoint.last_hash == hash)
            {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn assert_db_matches_anvil(writer: &PostgresChainWriter, http_url: &Url) {
    let head = rpc(http_url, "eth_blockNumber", json!([])).await;
    let head = parse_quantity(head.as_str().unwrap());
    for height in 0..=head {
        let remote = rpc(
            http_url,
            "eth_getBlockByNumber",
            json!([format_quantity(height), false]),
        )
        .await;
        let local = writer
            .canonical_header_at_height(height)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("missing local canonical header at height {height}"));
        assert_eq!(local.height, height);
        assert_eq!(local.hash, parse_hash(remote["hash"].as_str().unwrap()));
        assert_eq!(
            local.parent_hash,
            parse_hash(remote["parentHash"].as_str().unwrap())
        );
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

fn event_runtime_code() -> String {
    format!("0x7f{}60006000a100", TOPIC.trim_start_matches("0x"))
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn parse_quantity(value: &str) -> u64 {
    u64::from_str_radix(value.trim_start_matches("0x"), 16).unwrap()
}

fn format_quantity(value: u64) -> String {
    format!("0x{value:x}")
}

fn parse_hash(value: &str) -> [u8; 32] {
    let hex = value.trim_start_matches("0x");
    assert_eq!(hex.len(), 64);
    let mut output = [0_u8; 32];
    for index in 0..32 {
        output[index] = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).unwrap();
    }
    output
}
