use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use chainweave_core::{AppConfig, ChainIdentity, ValidationProfile};
use chainweave_rpc::RpcClient;
use clap::{Parser, Subcommand};
use tracing::info;
use tracing_subscriber::EnvFilter;
use url::Url;

#[derive(Debug, Parser)]
#[command(name = "chainweave", version, about = "Reorg-safe EVM chain indexer")]
struct Cli {
    #[arg(long, global = true, env = "CHAINWEAVE_CONFIG")]
    config: Option<PathBuf>,
    #[arg(long, global = true)]
    rpc_url: Option<Url>,
    #[arg(long, global = true, requires = "expected_genesis_hash")]
    expected_chain_id: Option<u64>,
    #[arg(long, global = true, requires = "expected_chain_id")]
    expected_genesis_hash: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Read and verify the current head from the configured primary RPC.
    Head,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing()?;
    let cli = Cli::parse();
    let mut config = AppConfig::load(cli.config.as_deref()).context("configuration load failed")?;
    apply_cli_overrides(&mut config, &cli)?;

    match cli.command {
        Command::Head => run_head(&config).await,
    }
}

fn apply_cli_overrides(config: &mut AppConfig, cli: &Cli) -> Result<()> {
    if let Some(url) = &cli.rpc_url {
        config.rpc.primary_url.clone_from(url);
    }
    match (&cli.expected_chain_id, &cli.expected_genesis_hash) {
        (Some(chain_id), Some(genesis_hash)) => {
            config.expected_chain = Some(ChainIdentity {
                chain_id: *chain_id,
                genesis_hash: genesis_hash.clone(),
            });
        }
        (None, None) => {}
        _ => bail!("expected chain ID and genesis hash must be provided together"),
    }
    Ok(())
}

async fn run_head(config: &AppConfig) -> Result<()> {
    config
        .validate(ValidationProfile::Head)
        .context("configuration validation failed")?;
    let client = RpcClient::connect(&config.rpc.primary_url).await?;
    let head = client.head().await?;
    if let Some(expected) = &config.expected_chain {
        RpcClient::verify_identity(&head, expected)?;
    }

    info!(
        chain_id = head.chain_id,
        block_number = head.number,
        "read current chain head"
    );
    println!("{}", serde_json::to_string_pretty(&head)?);
    Ok(())
}

fn init_tracing() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|error| anyhow::anyhow!("failed to initialize tracing: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_identity_and_rpc_override_loaded_configuration() {
        let cli = Cli::try_parse_from([
            "chainweave",
            "--rpc-url",
            "wss://rpc.example.com/ws",
            "--expected-chain-id",
            "11155111",
            "--expected-genesis-hash",
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "head",
        ])
        .unwrap();
        let mut config = AppConfig::default();

        apply_cli_overrides(&mut config, &cli).unwrap();

        assert_eq!(config.rpc.primary_url.as_str(), "wss://rpc.example.com/ws");
        assert_eq!(config.expected_chain.unwrap().chain_id, 11_155_111);
    }
}
