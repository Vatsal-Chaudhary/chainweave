use std::{net::SocketAddr, path::Path};

use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

const ENV_PREFIX: &str = "CHAINWEAVE_";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    pub rpc: RpcConfig,
    pub indexer: IndexerConfig,
    pub abi: AbiConfig,
    pub kafka: KafkaConfig,
    pub server: ServerConfig,
    pub database_url: Option<String>,
    /// Compatibility shim for the original top-level Kafka broker setting.
    pub kafka_brokers: Option<Vec<String>>,
    pub expected_chain: Option<ChainIdentity>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RpcConfig {
    pub primary_url: Url,
    pub verifier_url: Option<Url>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IndexerConfig {
    pub header_cache_size: usize,
    pub max_reorg_depth: u64,
    pub safe_depth: Option<u64>,
    pub finalized_depth: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct AbiConfig {
    pub enabled: bool,
    pub contracts: Vec<AbiContractConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct AbiContractConfig {
    pub address: String,
    pub standard: String,
    pub decoder_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    pub outbox_topic: String,
    pub consumer_group: String,
    pub queue_buffering_max_messages: usize,
    pub delivery_timeout_ms: u64,
    pub dispatcher_poll_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    pub listen_addr: SocketAddr,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ChainIdentity {
    pub chain_id: u64,
    pub genesis_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationProfile {
    Head,
    Kafka,
    Workers,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveStartPoint {
    Explicit(u64),
    DurableCheckpoint,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to load configuration: {0}")]
    Load(#[source] Box<figment::Error>),
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            rpc: RpcConfig {
                primary_url: Url::parse("http://127.0.0.1:8545").expect("default URL is valid"),
                verifier_url: None,
            },
            indexer: IndexerConfig {
                header_cache_size: 256,
                max_reorg_depth: 2_048,
                safe_depth: None,
                finalized_depth: None,
            },
            abi: AbiConfig::default(),
            kafka: KafkaConfig {
                brokers: Vec::new(),
                outbox_topic: "chainweave.outbox".to_owned(),
                consumer_group: "chainweave-demo-consumer".to_owned(),
                queue_buffering_max_messages: 10_000,
                delivery_timeout_ms: 30_000,
                dispatcher_poll_ms: 1_000,
            },
            server: ServerConfig {
                listen_addr: "127.0.0.1:9100".parse().expect("default address is valid"),
            },
            database_url: None,
            kafka_brokers: None,
            expected_chain: None,
        }
    }
}

impl AppConfig {
    /// Loads defaults, an optional TOML file, and `CHAINWEAVE_*` environment values.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Load`] when a source cannot be parsed or deserialized.
    pub fn load(path: Option<&Path>) -> Result<Self, ConfigError> {
        let mut figment = Figment::from(Serialized::defaults(Self::default()));
        if let Some(path) = path {
            figment = figment.merge(Toml::file(path));
        }

        // Double underscores map environment variables onto nested config fields.
        let config: Self = figment
            .merge(Env::prefixed(ENV_PREFIX).split("__"))
            .extract()
            .map_err(|error| ConfigError::Load(Box::new(error)))?;
        Ok(config)
    }

    /// Validates structural settings and secrets required by the selected process profile.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Invalid`] when a URL, depth, capacity, identity, or required
    /// worker secret is invalid.
    pub fn validate(&self, profile: ValidationProfile) -> Result<(), ConfigError> {
        validate_rpc_url("rpc.primary_url", &self.rpc.primary_url)?;
        if let Some(url) = &self.rpc.verifier_url {
            validate_rpc_url("rpc.verifier_url", url)?;
        }

        if self.indexer.header_cache_size == 0 {
            return Err(invalid("indexer.header_cache_size must be nonzero"));
        }
        if self.indexer.max_reorg_depth == 0 {
            return Err(invalid("indexer.max_reorg_depth must be nonzero"));
        }
        if self.indexer.max_reorg_depth < self.indexer.header_cache_size as u64 {
            return Err(invalid(
                "indexer.max_reorg_depth must be at least indexer.header_cache_size",
            ));
        }
        if let (Some(safe), Some(finalized)) =
            (self.indexer.safe_depth, self.indexer.finalized_depth)
            && safe > finalized
        {
            return Err(invalid(
                "indexer.safe_depth must not exceed indexer.finalized_depth",
            ));
        }

        if let Some(identity) = &self.expected_chain {
            if identity.chain_id == 0 {
                return Err(invalid("expected_chain.chain_id must be nonzero"));
            }
            validate_hash(&identity.genesis_hash)?;
        }
        validate_abi_config(&self.abi)?;

        if profile == ValidationProfile::Workers {
            let database_url = self
                .database_url
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| invalid("database_url is required before starting workers"))?;
            validate_database_url(database_url)?;
        }

        if matches!(
            profile,
            ValidationProfile::Kafka | ValidationProfile::Workers
        ) {
            validate_kafka_config(self)?;
        }

        Ok(())
    }

    #[must_use]
    pub fn kafka_brokers(&self) -> &[String] {
        self.kafka_brokers
            .as_deref()
            .filter(|brokers| !brokers.is_empty())
            .unwrap_or(&self.kafka.brokers)
    }

    /// Validates live-worker settings that cannot be inferred from the general worker profile.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Invalid`] when live mode has no durable or explicit start point.
    pub fn validate_live_start(start_point: Option<LiveStartPoint>) -> Result<(), ConfigError> {
        if start_point.is_none() {
            return Err(invalid(
                "live mode requires a durable checkpoint or an explicit --start-block",
            ));
        }
        Ok(())
    }
}

/// Redacts URL secrets before they are logged, returned in errors, or exposed through health.
///
/// This intentionally handles common RPC URL shapes such as `/v2/<key>`, `/v3/<key>`, and query
/// parameters named `api_key`, `key`, `token`, `secret`, or `password`.
#[must_use]
pub fn redact_url(url: &Url) -> String {
    let mut redacted = url.clone();
    if redacted.password().is_some() {
        let _ = redacted.set_password(Some("redacted"));
    }

    let mut path_segments = redacted
        .path_segments()
        .map(|segments| segments.map(ToOwned::to_owned).collect::<Vec<_>>())
        .unwrap_or_default();
    for index in 0..path_segments.len() {
        let previous = index
            .checked_sub(1)
            .and_then(|previous| path_segments.get(previous))
            .map_or_else(String::new, Clone::clone)
            .to_ascii_lowercase();
        let current = path_segments[index].to_ascii_lowercase();
        if matches!(previous.as_str(), "v2" | "v3" | "key" | "keys")
            || current.contains("apikey")
            || current.contains("api-key")
        {
            "redacted".clone_into(&mut path_segments[index]);
        }
    }
    if !path_segments.is_empty()
        && let Ok(mut segments) = redacted.path_segments_mut()
    {
        segments
            .clear()
            .extend(path_segments.iter().map(String::as_str));
    }

    let query_pairs = redacted
        .query_pairs()
        .map(|(key, value)| {
            let lower = key.to_ascii_lowercase();
            let value = if is_secret_query_key(&lower) {
                "redacted".into()
            } else {
                value
            };
            (key.into_owned(), value.into_owned())
        })
        .collect::<Vec<_>>();
    if redacted.query().is_some() {
        redacted.query_pairs_mut().clear().extend_pairs(query_pairs);
    }

    redacted.to_string()
}

fn is_secret_query_key(key: &str) -> bool {
    key == "key"
        || key == "api_key"
        || key == "apikey"
        || key == "api-key"
        || key.ends_with("_key")
        || key.contains("token")
        || key.contains("secret")
        || key.contains("password")
}

fn validate_rpc_url(name: &str, url: &Url) -> Result<(), ConfigError> {
    if !matches!(url.scheme(), "http" | "https" | "ws" | "wss") {
        return Err(invalid(format!("{name} must use http, https, ws, or wss")));
    }
    if url.host_str().is_none() {
        return Err(invalid(format!("{name} must include a host")));
    }
    Ok(())
}

fn validate_database_url(value: &str) -> Result<(), ConfigError> {
    let url = Url::parse(value).map_err(|_| invalid("database_url must be a valid URL"))?;
    if !matches!(url.scheme(), "postgres" | "postgresql") {
        return Err(invalid("database_url must use postgres or postgresql"));
    }
    if url.password().is_none_or(str::is_empty) && !uses_local_socket(&url) {
        return Err(invalid("database_url must include a password"));
    }
    Ok(())
}

fn uses_local_socket(url: &Url) -> bool {
    url.query_pairs()
        .any(|(key, value)| key == "host" && value.starts_with('/'))
}

fn validate_hash(value: &str) -> Result<(), ConfigError> {
    let bytes = value.strip_prefix("0x").unwrap_or(value);
    if bytes.len() != 64 || !bytes.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid(
            "expected_chain.genesis_hash must be a 32-byte hex value",
        ));
    }
    Ok(())
}

fn validate_abi_config(config: &AbiConfig) -> Result<(), ConfigError> {
    for contract in &config.contracts {
        validate_address(&contract.address)?;
        validate_abi_standard(&contract.standard)?;
        if let Some(version) = &contract.decoder_version
            && version.trim().is_empty()
        {
            return Err(invalid("abi.contracts.decoder_version must not be empty"));
        }
    }
    Ok(())
}

fn validate_kafka_config(config: &AppConfig) -> Result<(), ConfigError> {
    let brokers = config.kafka_brokers();
    if brokers.iter().any(|broker| broker.trim().is_empty()) {
        return Err(invalid("kafka brokers must not contain empty entries"));
    }
    if config.kafka.outbox_topic.trim().is_empty() {
        return Err(invalid("kafka.outbox_topic must not be empty"));
    }
    if config.kafka.consumer_group.trim().is_empty() {
        return Err(invalid("kafka.consumer_group must not be empty"));
    }
    if config.kafka.queue_buffering_max_messages == 0 {
        return Err(invalid(
            "kafka.queue_buffering_max_messages must be nonzero",
        ));
    }
    if config.kafka.delivery_timeout_ms == 0 {
        return Err(invalid("kafka.delivery_timeout_ms must be nonzero"));
    }
    if config.kafka.dispatcher_poll_ms == 0 {
        return Err(invalid("kafka.dispatcher_poll_ms must be nonzero"));
    }
    Ok(())
}

fn validate_address(value: &str) -> Result<(), ConfigError> {
    let bytes = value.strip_prefix("0x").unwrap_or(value);
    if bytes.len() != 40 || !bytes.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid("abi.contracts.address must be a 20-byte hex value"));
    }
    Ok(())
}

fn validate_abi_standard(value: &str) -> Result<(), ConfigError> {
    match value
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-'], "_")
        .as_str()
    {
        "erc20" | "erc_20" | "erc721" | "erc_721" | "uniswap_v3" | "uniswap_v3_pool"
        | "uniswapv3pool" => Ok(()),
        _ => Err(invalid(
            "abi.contracts.standard must be erc20, erc721, or uniswap_v3_pool",
        )),
    }
}

fn invalid(message: impl Into<String>) -> ConfigError {
    ConfigError::Invalid(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid_for_head_command() {
        AppConfig::default()
            .validate(ValidationProfile::Head)
            .unwrap();
    }

    #[test]
    fn workers_require_database_credentials() {
        let error = AppConfig::default()
            .validate(ValidationProfile::Workers)
            .unwrap_err();
        assert!(error.to_string().contains("database_url is required"));
    }

    #[test]
    fn rejects_invalid_depth_relationships() {
        let mut config = AppConfig::default();
        config.indexer.max_reorg_depth = 100;
        assert!(config.validate(ValidationProfile::Head).is_err());

        config.indexer.max_reorg_depth = 2_048;
        config.indexer.safe_depth = Some(65);
        config.indexer.finalized_depth = Some(64);
        assert!(config.validate(ValidationProfile::Head).is_err());

        config.indexer.safe_depth = Some(12);
        config.validate(ValidationProfile::Head).unwrap();
    }

    #[test]
    fn rejects_unsupported_rpc_scheme_and_incomplete_database_secret() {
        let mut config = AppConfig::default();
        config.rpc.primary_url = Url::parse("ftp://rpc.example.com").unwrap();
        assert!(config.validate(ValidationProfile::Head).is_err());

        config.rpc.primary_url = Url::parse("https://rpc.example.com").unwrap();
        config.database_url = Some("postgres://db.example.com/chainweave".to_owned());
        let error = config.validate(ValidationProfile::Workers).unwrap_err();
        assert!(error.to_string().contains("must include a password"));

        config.database_url = Some("postgresql://vatsal@localhost/postgres?host=/tmp".to_owned());
        config.validate(ValidationProfile::Workers).unwrap();
    }

    #[test]
    fn rejects_malformed_genesis_hash() {
        let config = AppConfig {
            expected_chain: Some(ChainIdentity {
                chain_id: 1,
                genesis_hash: "0x1234".to_owned(),
            }),
            ..AppConfig::default()
        };
        assert!(config.validate(ValidationProfile::Head).is_err());
    }

    #[test]
    fn validates_abi_registry_entries() {
        let mut config = AppConfig {
            abi: AbiConfig {
                enabled: true,
                contracts: vec![AbiContractConfig {
                    address: "0x1111111111111111111111111111111111111111".to_owned(),
                    standard: "erc20".to_owned(),
                    decoder_version: Some("erc20:v1".to_owned()),
                }],
            },
            ..AppConfig::default()
        };
        config.validate(ValidationProfile::Head).unwrap();

        config.abi.contracts[0].standard = "nonsense".to_owned();
        let error = config.validate(ValidationProfile::Head).unwrap_err();
        assert!(error.to_string().contains("abi.contracts.standard"));
    }

    #[test]
    fn validates_minimal_kafka_config() {
        let mut config = AppConfig {
            database_url: Some("postgresql://vatsal@localhost/postgres?host=/tmp".to_owned()),
            ..AppConfig::default()
        };
        config.validate(ValidationProfile::Workers).unwrap();

        config.kafka.brokers = vec!["127.0.0.1:9092".to_owned()];
        config.kafka.outbox_topic = "chainweave.outbox".to_owned();
        config.validate(ValidationProfile::Workers).unwrap();

        config.kafka.queue_buffering_max_messages = 0;
        let error = config.validate(ValidationProfile::Workers).unwrap_err();
        assert!(error.to_string().contains("queue_buffering_max_messages"));
    }

    #[test]
    fn rejects_live_mode_without_start_point() {
        let error = AppConfig::validate_live_start(None).unwrap_err();

        assert!(error.to_string().contains("explicit --start-block"));
        AppConfig::validate_live_start(Some(LiveStartPoint::Explicit(10))).unwrap();
        AppConfig::validate_live_start(Some(LiveStartPoint::DurableCheckpoint)).unwrap();
    }

    #[test]
    fn redact_url_masks_keys_in_path_and_query() {
        let path_key = Url::parse("https://example.rpc/v3/super-secret-key?chain=sepolia").unwrap();
        assert_eq!(
            redact_url(&path_key),
            "https://example.rpc/v3/redacted?chain=sepolia"
        );

        let query_key =
            Url::parse("https://example.rpc/mainnet?api_key=super-secret-key&chain=1").unwrap();
        assert_eq!(
            redact_url(&query_key),
            "https://example.rpc/mainnet?api_key=redacted&chain=1"
        );
    }
}
