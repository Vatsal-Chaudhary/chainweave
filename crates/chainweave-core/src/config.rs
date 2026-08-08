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
    pub queues: QueueConfig,
    pub server: ServerConfig,
    pub database_url: Option<String>,
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QueueConfig {
    pub fetch: usize,
    pub coordinate: usize,
    pub decode: usize,
    pub write: usize,
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
    Workers,
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
            queues: QueueConfig {
                fetch: 128,
                coordinate: 64,
                decode: 128,
                write: 64,
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

        for (name, capacity) in [
            ("queues.fetch", self.queues.fetch),
            ("queues.coordinate", self.queues.coordinate),
            ("queues.decode", self.queues.decode),
            ("queues.write", self.queues.write),
        ] {
            if capacity == 0 {
                return Err(invalid(format!("{name} must be nonzero")));
            }
        }

        if let Some(identity) = &self.expected_chain {
            if identity.chain_id == 0 {
                return Err(invalid("expected_chain.chain_id must be nonzero"));
            }
            validate_hash(&identity.genesis_hash)?;
        }

        if profile == ValidationProfile::Workers {
            let database_url = self
                .database_url
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| invalid("database_url is required before starting workers"))?;
            validate_database_url(database_url)?;

            if let Some(brokers) = &self.kafka_brokers
                && (brokers.is_empty() || brokers.iter().any(|broker| broker.trim().is_empty()))
            {
                return Err(invalid("kafka_brokers must not contain empty entries"));
            }
        }

        Ok(())
    }
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
    if url.password().is_none_or(str::is_empty) {
        return Err(invalid("database_url must include a password"));
    }
    Ok(())
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
    fn rejects_invalid_depth_relationships_and_queue_capacity() {
        let mut config = AppConfig::default();
        config.indexer.max_reorg_depth = 100;
        assert!(config.validate(ValidationProfile::Head).is_err());

        config.indexer.max_reorg_depth = 2_048;
        config.indexer.safe_depth = Some(65);
        config.indexer.finalized_depth = Some(64);
        assert!(config.validate(ValidationProfile::Head).is_err());

        config.indexer.safe_depth = Some(12);
        config.queues.write = 0;
        assert!(config.validate(ValidationProfile::Head).is_err());
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
}
