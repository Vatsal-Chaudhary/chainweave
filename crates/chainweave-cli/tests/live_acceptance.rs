use std::{
    fs,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chainweave_core::redact_url;
use chainweave_rpc::RpcClient;
use url::Url;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "long-running live acceptance; run through make test-live-acceptance"]
async fn live_acceptance_runs_for_duration_with_rss_bound() {
    let primary_ws_url = env_url("LIVE_ACCEPTANCE_PRIMARY_WS_URL");
    let verifier_url = optional_env_url("LIVE_ACCEPTANCE_VERIFIER_URL");
    let expected_chain_id = env_u64("LIVE_ACCEPTANCE_EXPECTED_CHAIN_ID");
    let database_url = env_string("LIVE_ACCEPTANCE_DATABASE_URL");
    let poll_interval_ms = env_u64_or("LIVE_ACCEPTANCE_POLL_INTERVAL_MS", 12_000);
    let budget_window_secs = env_u64_or("LIVE_ACCEPTANCE_BUDGET_WINDOW_SECS", 60);
    let duration_secs = env_u64("LIVE_ACCEPTANCE_DURATION_SECS");
    let rss_bound_mb = env_u64("LIVE_ACCEPTANCE_RSS_BOUND_MB");
    let warmup_secs = env_u64_or(
        "LIVE_ACCEPTANCE_WARMUP_SECS",
        duration_secs.saturating_div(10).clamp(1, 60),
    );

    let client = RpcClient::connect(&primary_ws_url).await.unwrap();
    let head = client.head().await.unwrap();
    assert_eq!(head.chain_id, expected_chain_id);

    let workspace = TestWorkspace::new();
    let config_path = workspace.config_path(
        &database_url,
        &primary_ws_url,
        verifier_url.as_ref(),
        expected_chain_id,
        &head.genesis_hash.to_string(),
    );

    let mut runner = RunnerProcess::spawn(
        &config_path,
        head.number,
        poll_interval_ms,
        budget_window_secs,
    );
    let started_at = tokio::time::Instant::now();
    let warmup_at = started_at + Duration::from_secs(warmup_secs);
    let deadline = started_at + Duration::from_secs(duration_secs);
    let mut max_rss_kb = 0_u64;

    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        if now >= warmup_at {
            let rss = rss_kb(runner.pid()).expect("runner RSS must be readable");
            max_rss_kb = max_rss_kb.max(rss);
            assert!(
                rss_within_bound(rss, rss_bound_mb),
                "runner RSS {} MiB exceeded bound {} MiB",
                rss / 1024,
                rss_bound_mb
            );
        }
        tokio::time::sleep(Duration::from_secs(5).min(deadline - now)).await;
    }

    runner.kill();
    println!(
        "live acceptance completed: duration_secs={duration_secs} warmup_secs={warmup_secs} max_rss_mib={} primary={} verifier={}",
        max_rss_kb / 1024,
        redact_url(&primary_ws_url),
        verifier_url
            .as_ref()
            .map(redact_url)
            .unwrap_or_else(|| "none".to_owned())
    );
}

struct TestWorkspace {
    path: PathBuf,
}

impl TestWorkspace {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "chainweave-live-acceptance-{}-{}",
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
        primary_ws_url: &Url,
        verifier_url: Option<&Url>,
        chain_id: u64,
        genesis_hash: &str,
    ) -> PathBuf {
        let path = self.path.join("chainweave.toml");
        let verifier = verifier_url
            .map(|url| format!("verifier_url = \"{url}\"\n"))
            .unwrap_or_default();
        fs::write(
            &path,
            format!(
                r#"
database_url = "{database_url}"

[rpc]
primary_url = "{primary_ws_url}"
{verifier}
[indexer]
header_cache_size = 256
max_reorg_depth = 2048

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

struct RunnerProcess {
    child: Child,
}

impl RunnerProcess {
    fn spawn(
        config_path: &PathBuf,
        start_block: u64,
        poll_interval_ms: u64,
        budget_window_secs: u64,
    ) -> Self {
        let binary = env!("CARGO_BIN_EXE_chainweave");
        let child = Command::new(binary)
            .args([
                "--config",
                config_path.to_str().unwrap(),
                "live",
                "--start-block",
                &start_block.to_string(),
                "--poll-interval-ms",
                &poll_interval_ms.to_string(),
                "--budget-window-secs",
                &budget_window_secs.to_string(),
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

    fn pid(&self) -> u32 {
        self.child.id()
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

fn rss_kb(pid: u32) -> Option<u64> {
    let status = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status.lines().find_map(|line| {
        line.strip_prefix("VmRSS:")
            .and_then(|value| value.split_whitespace().next())
            .and_then(|value| value.parse::<u64>().ok())
    })
}

fn rss_within_bound(rss_kb: u64, bound_mb: u64) -> bool {
    rss_kb <= bound_mb.saturating_mul(1024)
}

fn env_string(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set"))
}

fn env_url(name: &str) -> Url {
    Url::parse(&env_string(name)).unwrap_or_else(|error| panic!("{name} must be a URL: {error}"))
}

fn optional_env_url(name: &str) -> Option<Url> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            Url::parse(&value).unwrap_or_else(|error| panic!("{name} must be a URL: {error}"))
        })
}

fn env_u64(name: &str) -> u64 {
    env_string(name)
        .parse()
        .unwrap_or_else(|error| panic!("{name} must be an unsigned integer: {error}"))
}

fn env_u64_or(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            value
                .parse()
                .unwrap_or_else(|error| panic!("{name} must be an unsigned integer: {error}"))
        })
        .unwrap_or(default)
}

#[test]
fn rss_bound_check_is_inclusive_and_rejects_overage() {
    assert!(rss_within_bound(512 * 1024, 512));
    assert!(!rss_within_bound(512 * 1024 + 1, 512));
}
