use std::{collections::BTreeMap, fmt::Write as _};

use alloy::{
    dyn_abi::{DynSolValue, EventExt},
    json_abi::{Event, EventParam},
    primitives::{Address, B256},
};
use metrics::counter;
use serde_json::{Map, Value, json};
use thiserror::Error;

use crate::RawLog;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbiRegistry {
    contracts: BTreeMap<[u8; 20], ContractDecoder>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractDecoder {
    standard: AbiStandard,
    decoder_version: String,
    events: Vec<Event>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbiStandard {
    Erc20,
    Erc721,
    UniswapV3Pool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbiRegistryEntry {
    pub address: [u8; 20],
    pub standard: AbiStandard,
    pub decoder_version: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeStatus {
    Decoded,
    UnknownAbi,
    UnknownSignature,
    DecodeFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DecodeReport {
    pub decoded: usize,
    pub unknown_abi: usize,
    pub unknown_signature: usize,
    pub decode_failures: usize,
}

#[derive(Debug, Error)]
pub enum AbiRegistryError {
    #[error("invalid ABI standard {0}")]
    InvalidStandard(String),
    #[error("invalid EVM address {0}: {1}")]
    InvalidAddress(String, String),
    #[error("invalid bundled event definition {definition}: {reason}")]
    InvalidBundledEvent {
        definition: &'static str,
        reason: String,
    },
    #[error("duplicate ABI registry address {0}")]
    DuplicateAddress(String),
}

impl AbiRegistry {
    /// Builds a deterministic address-to-decoder registry from config entries.
    ///
    /// # Errors
    ///
    /// Returns an error when a bundled ABI cannot be parsed or an address is duplicated.
    pub fn new(
        entries: impl IntoIterator<Item = AbiRegistryEntry>,
    ) -> Result<Self, AbiRegistryError> {
        let mut contracts = BTreeMap::new();
        for entry in entries {
            let decoder = ContractDecoder::new(entry.standard, entry.decoder_version)?;
            if contracts.insert(entry.address, decoder).is_some() {
                return Err(AbiRegistryError::DuplicateAddress(hex_address(
                    &entry.address,
                )));
            }
        }
        Ok(Self { contracts })
    }

    /// Builds a registry from stringly typed config values.
    ///
    /// # Errors
    ///
    /// Returns an error when an address, standard, or bundled ABI is invalid.
    pub fn from_config_entries<'a>(
        entries: impl IntoIterator<Item = (&'a str, &'a str, Option<&'a str>)>,
    ) -> Result<Self, AbiRegistryError> {
        entries
            .into_iter()
            .map(|(address, standard, version)| {
                let standard = standard.parse()?;
                Ok(AbiRegistryEntry {
                    address: parse_address(address)?,
                    standard,
                    decoder_version: version
                        .map(str::to_owned)
                        .unwrap_or_else(|| default_decoder_version(standard)),
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .and_then(Self::new)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.contracts.is_empty()
    }

    /// Decodes one log in place without failing ingestion.
    pub fn decode_log_in_place(&self, log: &mut RawLog) -> DecodeStatus {
        match self.decode_log(log) {
            DecodeResult::Decoded {
                decoded_event,
                decoder_version,
            } => {
                log.decoded_event = Some(decoded_event);
                log.decoder_version = Some(decoder_version);
                record_decode_status(DecodeStatus::Decoded);
                DecodeStatus::Decoded
            }
            DecodeResult::UnknownAbi => {
                log.decoded_event = None;
                log.decoder_version = None;
                record_decode_status(DecodeStatus::UnknownAbi);
                DecodeStatus::UnknownAbi
            }
            DecodeResult::UnknownSignature => {
                log.decoded_event = None;
                log.decoder_version = None;
                record_decode_status(DecodeStatus::UnknownSignature);
                DecodeStatus::UnknownSignature
            }
            DecodeResult::DecodeFailure => {
                log.decoded_event = None;
                log.decoder_version = None;
                record_decode_status(DecodeStatus::DecodeFailure);
                DecodeStatus::DecodeFailure
            }
        }
    }

    /// Decodes all logs in order and records aggregate non-blocking outcomes.
    pub fn decode_logs_in_place<'a>(
        &self,
        logs: impl IntoIterator<Item = &'a mut RawLog>,
    ) -> DecodeReport {
        let mut report = DecodeReport::default();
        for log in logs {
            report.record(self.decode_log_in_place(log));
        }
        report
    }

    fn decode_log(&self, log: &RawLog) -> DecodeResult {
        let Some(decoder) = self.contracts.get(&log.address) else {
            return DecodeResult::UnknownAbi;
        };
        let Some(topic0) = log.topics.first().copied() else {
            return DecodeResult::UnknownSignature;
        };

        let topics = log
            .topics
            .iter()
            .copied()
            .map(B256::from)
            .collect::<Vec<_>>();
        let mut saw_selector = false;
        let mut saw_decode_failure = false;
        for event in decoder
            .events
            .iter()
            .filter(|event| event.selector() == B256::from(topic0))
        {
            saw_selector = true;
            match event.decode_log_parts(topics.iter().copied(), &log.data) {
                Ok(decoded) => {
                    let decoded_event = decoded_event_json(decoder, event, &decoded);
                    return DecodeResult::Decoded {
                        decoded_event,
                        decoder_version: decoder.decoder_version.clone(),
                    };
                }
                Err(_) => {
                    saw_decode_failure = true;
                }
            }
        }

        if saw_decode_failure {
            DecodeResult::DecodeFailure
        } else if saw_selector {
            DecodeResult::DecodeFailure
        } else {
            DecodeResult::UnknownSignature
        }
    }
}

impl ContractDecoder {
    fn new(standard: AbiStandard, decoder_version: String) -> Result<Self, AbiRegistryError> {
        let events = standard
            .event_signatures()
            .into_iter()
            .map(parse_event)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            standard,
            decoder_version,
            events,
        })
    }
}

impl AbiStandard {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Erc20 => "erc20",
            Self::Erc721 => "erc721",
            Self::UniswapV3Pool => "uniswap_v3_pool",
        }
    }

    fn event_signatures(self) -> Vec<&'static str> {
        match self {
            Self::Erc20 => vec![
                "event Transfer(address indexed from,address indexed to,uint256 value)",
                "event Approval(address indexed owner,address indexed spender,uint256 value)",
            ],
            Self::Erc721 => vec![
                "event Transfer(address indexed from,address indexed to,uint256 indexed tokenId)",
                "event Approval(address indexed owner,address indexed approved,uint256 indexed tokenId)",
            ],
            Self::UniswapV3Pool => vec![
                "event Swap(address indexed sender,address indexed recipient,int256 amount0,int256 amount1,uint160 sqrtPriceX96,uint128 liquidity,int24 tick)",
                "event Mint(address sender,address indexed owner,int24 indexed tickLower,int24 indexed tickUpper,uint128 amount,uint256 amount0,uint256 amount1)",
                "event Burn(address indexed owner,int24 indexed tickLower,int24 indexed tickUpper,uint128 amount,uint256 amount0,uint256 amount1)",
            ],
        }
    }
}

impl std::str::FromStr for AbiStandard {
    type Err = AbiRegistryError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match normalized_standard(value).as_str() {
            "erc20" | "erc_20" | "erc-20" => Ok(Self::Erc20),
            "erc721" | "erc_721" | "erc-721" => Ok(Self::Erc721),
            "uniswapv3pool" | "uniswap_v3_pool" | "uniswap-v3-pool" | "uniswap_v3" => {
                Ok(Self::UniswapV3Pool)
            }
            _ => Err(AbiRegistryError::InvalidStandard(value.to_owned())),
        }
    }
}

impl DecodeReport {
    pub(crate) fn record(&mut self, status: DecodeStatus) {
        match status {
            DecodeStatus::Decoded => self.decoded += 1,
            DecodeStatus::UnknownAbi => self.unknown_abi += 1,
            DecodeStatus::UnknownSignature => self.unknown_signature += 1,
            DecodeStatus::DecodeFailure => self.decode_failures += 1,
        }
    }
}

enum DecodeResult {
    Decoded {
        decoded_event: Value,
        decoder_version: String,
    },
    UnknownAbi,
    UnknownSignature,
    DecodeFailure,
}

fn parse_event(definition: &'static str) -> Result<Event, AbiRegistryError> {
    Event::parse(definition).map_err(|error| AbiRegistryError::InvalidBundledEvent {
        definition,
        reason: error.to_string(),
    })
}

fn decoded_event_json(
    decoder: &ContractDecoder,
    event: &Event,
    decoded: &alloy::dyn_abi::DecodedEvent,
) -> Value {
    let mut indexed = decoded.indexed.iter();
    let mut body = decoded.body.iter();
    let mut args = Map::new();
    let mut ordered_args = Vec::with_capacity(event.inputs.len());

    for (position, input) in event.inputs.iter().enumerate() {
        let value = if input.indexed {
            indexed.next()
        } else {
            body.next()
        };
        let value = value.map(sol_value_json).unwrap_or(Value::Null);
        let name = parameter_name(input, position);
        args.insert(name.clone(), value.clone());
        ordered_args.push(json!({
            "name": name,
            "type": input.ty,
            "indexed": input.indexed,
            "value": value,
        }));
    }

    json!({
        "schema_version": 1,
        "standard": decoder.standard.as_str(),
        "decoder_version": decoder.decoder_version,
        "event": event.name,
        "signature": event.signature(),
        "args": args,
        "ordered_args": ordered_args,
    })
}

fn parameter_name(input: &EventParam, position: usize) -> String {
    if input.name.is_empty() {
        format!("arg{position}")
    } else {
        input.name.clone()
    }
}

fn sol_value_json(value: &DynSolValue) -> Value {
    match value {
        DynSolValue::Bool(value) => Value::Bool(*value),
        DynSolValue::Int(value, _) => Value::String(value.to_string()),
        DynSolValue::Uint(value, _) => Value::String(value.to_string()),
        DynSolValue::FixedBytes(word, size) => Value::String(hex_bytes(&word[..*size])),
        DynSolValue::Address(value) => Value::String(value.to_string()),
        DynSolValue::Function(value) => Value::String(hex_bytes(value.as_slice())),
        DynSolValue::Bytes(value) => Value::String(hex_bytes(value)),
        DynSolValue::String(value) => Value::String(value.clone()),
        DynSolValue::Array(values)
        | DynSolValue::FixedArray(values)
        | DynSolValue::Tuple(values) => Value::Array(values.iter().map(sol_value_json).collect()),
    }
}

fn record_decode_status(status: DecodeStatus) {
    match status {
        DecodeStatus::Decoded => counter!("chainweave_decode_success_total").increment(1),
        DecodeStatus::UnknownAbi => counter!("chainweave_decode_unknown_abi_total").increment(1),
        DecodeStatus::UnknownSignature => {
            counter!("chainweave_decode_unknown_signature_total").increment(1);
        }
        DecodeStatus::DecodeFailure => {
            counter!("chainweave_decode_failure_total").increment(1);
            counter!("chainweave_decode_failures_total").increment(1);
        }
    }
}

fn default_decoder_version(standard: AbiStandard) -> String {
    format!("{}:v1", standard.as_str())
}

fn normalized_standard(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace([' ', '-'], "_")
}

fn parse_address(value: &str) -> Result<[u8; 20], AbiRegistryError> {
    value
        .parse::<Address>()
        .map(|address| {
            let mut bytes = [0_u8; 20];
            bytes.copy_from_slice(address.as_slice());
            bytes
        })
        .map_err(|error| AbiRegistryError::InvalidAddress(value.to_owned(), error.to_string()))
}

fn hex_address(value: &[u8; 20]) -> String {
    hex_bytes(value)
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(2 + bytes.len() * 2);
    output.push_str("0x");
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to string cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    const ERC20_ADDRESS: [u8; 20] = [0x20; 20];
    const ERC721_ADDRESS: [u8; 20] = [0x72; 20];
    const UNI_ADDRESS: [u8; 20] = [0x33; 20];
    const UNKNOWN_ADDRESS: [u8; 20] = [0x99; 20];
    const ERC20_TRANSFER_TOPIC0: &str =
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
    const ERC20_APPROVAL_TOPIC0: &str =
        "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925";
    const UNISWAP_V3_SWAP_TOPIC0: &str =
        "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
    const UNISWAP_V3_MINT_TOPIC0: &str =
        "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde";
    const UNISWAP_V3_BURN_TOPIC0: &str =
        "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c";
    const ADDRESS_01_TOPIC: &str =
        "0x0000000000000000000000000101010101010101010101010101010101010101";
    const ADDRESS_02_TOPIC: &str =
        "0x0000000000000000000000000202020202020202020202020202020202020202";
    const ADDRESS_03_TOPIC: &str =
        "0x0000000000000000000000000303030303030303030303030303030303030303";
    const ADDRESS_04_TOPIC: &str =
        "0x0000000000000000000000000404040404040404040404040404040404040404";
    const ADDRESS_10_TOPIC: &str =
        "0x0000000000000000000000001010101010101010101010101010101010101010";
    const ADDRESS_11_TOPIC: &str =
        "0x0000000000000000000000001111111111111111111111111111111111111111";
    const ADDRESS_20_TOPIC: &str =
        "0x0000000000000000000000002020202020202020202020202020202020202020";
    const ADDRESS_22_TOPIC: &str =
        "0x0000000000000000000000002222222222222222222222222222222222222222";
    const ADDRESS_30_TOPIC: &str =
        "0x0000000000000000000000003030303030303030303030303030303030303030";
    const ADDRESS_50_TOPIC: &str =
        "0x0000000000000000000000005050505050505050505050505050505050505050";
    const WORD_10: &str = "0x000000000000000000000000000000000000000000000000000000000000000a";
    const WORD_77: &str = "0x000000000000000000000000000000000000000000000000000000000000004d";
    const WORD_88: &str = "0x0000000000000000000000000000000000000000000000000000000000000058";
    const WORD_NEG_60: &str = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffc4";
    const WORD_POS_60: &str = "0x000000000000000000000000000000000000000000000000000000000000003c";
    const WORD_NEG_10: &str = "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff6";
    const WORD_POS_10: &str = "0x000000000000000000000000000000000000000000000000000000000000000a";
    const ERC20_TRANSFER_1000_DATA: &str =
        "0x00000000000000000000000000000000000000000000000000000000000003e8";
    const UNISWAP_SWAP_DATA: &str = "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffb00000000000000000000000000000000000000000000000000000000000000090000000000000000000000000000000000000000000000000000000000003039000000000000000000000000000000000000000000000000000000000000004dffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff88";
    const UNISWAP_MINT_DATA: &str = "0x0000000000000000000000004040404040404040404040404040404040404040000000000000000000000000000000000000000000000000000000000000005800000000000000000000000000000000000000000000000000000000000000030000000000000000000000000000000000000000000000000000000000000004";
    const UNISWAP_BURN_DATA: &str = "0x000000000000000000000000000000000000000000000000000000000000000500000000000000000000000000000000000000000000000000000000000000060000000000000000000000000000000000000000000000000000000000000007";

    #[test]
    fn decodes_pinned_erc20_transfer_fixture() {
        let registry = registry([
            (ERC20_ADDRESS, AbiStandard::Erc20, "erc20:v1"),
            (ERC721_ADDRESS, AbiStandard::Erc721, "erc721:v1"),
        ]);
        let mut log = fixture_log(
            ERC20_ADDRESS,
            &[ERC20_TRANSFER_TOPIC0, ADDRESS_11_TOPIC, ADDRESS_22_TOPIC],
            ERC20_TRANSFER_1000_DATA,
        );

        assert_eq!(
            registry.decode_log_in_place(&mut log),
            DecodeStatus::Decoded
        );
        assert_eq!(log.decoder_version.as_deref(), Some("erc20:v1"));
        let decoded = log.decoded_event.as_ref().unwrap();
        assert_eq!(decoded["standard"], "erc20");
        assert_eq!(decoded["event"], "Transfer");
        assert_eq!(decoded["args"]["from"], hex_bytes(&[0x11; 20]));
        assert_eq!(decoded["args"]["to"], hex_bytes(&[0x22; 20]));
        assert_eq!(decoded["args"]["value"], "1000");
        assert_eq!(decoded["ordered_args"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn address_registry_resolves_overlapping_transfer_signatures() {
        let registry = registry([
            (ERC20_ADDRESS, AbiStandard::Erc20, "erc20:v1"),
            (ERC721_ADDRESS, AbiStandard::Erc721, "erc721:v1"),
        ]);

        let mut erc20 = fixture_log(
            ERC20_ADDRESS,
            &[ERC20_TRANSFER_TOPIC0, ADDRESS_11_TOPIC, ADDRESS_22_TOPIC],
            WORD_10,
        );
        let mut erc721 = fixture_log(
            ERC721_ADDRESS,
            &[
                ERC20_TRANSFER_TOPIC0,
                ADDRESS_11_TOPIC,
                ADDRESS_22_TOPIC,
                WORD_10,
            ],
            "0x",
        );

        registry.decode_log_in_place(&mut erc20);
        registry.decode_log_in_place(&mut erc721);

        assert_eq!(erc20.decoded_event.as_ref().unwrap()["standard"], "erc20");
        assert_eq!(erc20.decoded_event.as_ref().unwrap()["args"]["value"], "10");
        assert_eq!(erc721.decoded_event.as_ref().unwrap()["standard"], "erc721");
        assert_eq!(
            erc721.decoded_event.as_ref().unwrap()["args"]["tokenId"],
            "10"
        );
    }

    #[test]
    fn decodes_erc20_and_erc721_approval_fixtures_and_preserves_order() {
        let registry = registry([
            (ERC20_ADDRESS, AbiStandard::Erc20, "erc20:v1"),
            (ERC721_ADDRESS, AbiStandard::Erc721, "erc721:v1"),
        ]);
        let mut logs = vec![
            fixture_log(
                ERC721_ADDRESS,
                &[
                    ERC20_APPROVAL_TOPIC0,
                    ADDRESS_01_TOPIC,
                    ADDRESS_02_TOPIC,
                    WORD_77,
                ],
                "0x",
            ),
            fixture_log(
                ERC20_ADDRESS,
                &[ERC20_APPROVAL_TOPIC0, ADDRESS_03_TOPIC, ADDRESS_04_TOPIC],
                WORD_88,
            ),
        ];
        logs[0].transaction_index = 1;
        logs[0].log_index = 7;
        logs[1].transaction_index = 1;
        logs[1].log_index = 8;

        let report = registry.decode_logs_in_place(&mut logs);

        assert_eq!(report.decoded, 2);
        assert_eq!(
            logs.iter()
                .map(|log| (log.transaction_index, log.log_index))
                .collect::<Vec<_>>(),
            vec![(1, 7), (1, 8)]
        );
        assert_eq!(
            logs[0].decoded_event.as_ref().unwrap()["standard"],
            "erc721"
        );
        assert_eq!(
            logs[0].decoded_event.as_ref().unwrap()["args"]["tokenId"],
            "77"
        );
        assert_eq!(logs[1].decoded_event.as_ref().unwrap()["standard"], "erc20");
        assert_eq!(
            logs[1].decoded_event.as_ref().unwrap()["args"]["value"],
            "88"
        );
    }

    #[test]
    fn unknown_signature_and_decode_failure_do_not_error() {
        let registry = registry([(ERC20_ADDRESS, AbiStandard::Erc20, "erc20:v1")]);
        let mut unknown = fixture_log(
            ERC20_ADDRESS,
            &["0x0000000000000000000000000000000000000000000000000000000000000063"],
            "0x",
        );
        let mut malformed = fixture_log(
            ERC20_ADDRESS,
            &[ERC20_TRANSFER_TOPIC0, ADDRESS_11_TOPIC, ADDRESS_22_TOPIC],
            "0x01",
        );

        assert_eq!(
            registry.decode_log_in_place(&mut unknown),
            DecodeStatus::UnknownSignature
        );
        assert_eq!(
            registry.decode_log_in_place(&mut malformed),
            DecodeStatus::DecodeFailure
        );
        assert!(unknown.decoded_event.is_none());
        assert!(malformed.decoded_event.is_none());

        let mut batch = vec![unknown, malformed];
        let report = registry.decode_logs_in_place(&mut batch);
        assert_eq!(report.unknown_signature, 1);
        assert_eq!(report.decode_failures, 1);
    }

    #[test]
    fn unknown_address_is_non_blocking_and_records_signal() {
        let registry = registry([(ERC20_ADDRESS, AbiStandard::Erc20, "erc20:v1")]);
        let mut log = fixture_log(
            UNKNOWN_ADDRESS,
            &[ERC20_TRANSFER_TOPIC0, ADDRESS_11_TOPIC, ADDRESS_22_TOPIC],
            ERC20_TRANSFER_1000_DATA,
        );

        assert_eq!(
            registry.decode_log_in_place(&mut log),
            DecodeStatus::UnknownAbi
        );
        assert!(log.decoded_event.is_none());
        assert!(log.decoder_version.is_none());

        let report = registry.decode_logs_in_place([&mut log]);
        assert_eq!(report.unknown_abi, 1);
        assert_eq!(report.decoded, 0);
    }

    #[test]
    fn default_decoder_version_uses_canonical_standard_name() {
        for alias in ["erc20", "erc_20", "erc-20"] {
            let registry = AbiRegistry::from_config_entries([(
                "0x2020202020202020202020202020202020202020",
                alias,
                None,
            )])
            .unwrap();
            let mut log = fixture_log(
                ERC20_ADDRESS,
                &[ERC20_TRANSFER_TOPIC0, ADDRESS_11_TOPIC, ADDRESS_22_TOPIC],
                ERC20_TRANSFER_1000_DATA,
            );

            assert_eq!(
                registry.decode_log_in_place(&mut log),
                DecodeStatus::Decoded
            );
            assert_eq!(log.decoder_version.as_deref(), Some("erc20:v1"));
        }
    }

    #[test]
    fn decodes_pinned_uniswap_v3_swap_mint_and_burn_fixtures() {
        let registry = registry([(UNI_ADDRESS, AbiStandard::UniswapV3Pool, "uniswap:v1")]);

        let mut swap = fixture_log(
            UNI_ADDRESS,
            &[UNISWAP_V3_SWAP_TOPIC0, ADDRESS_10_TOPIC, ADDRESS_20_TOPIC],
            UNISWAP_SWAP_DATA,
        );
        let mut mint = fixture_log(
            UNI_ADDRESS,
            &[
                UNISWAP_V3_MINT_TOPIC0,
                ADDRESS_30_TOPIC,
                WORD_NEG_60,
                WORD_POS_60,
            ],
            UNISWAP_MINT_DATA,
        );
        let mut burn = fixture_log(
            UNI_ADDRESS,
            &[
                UNISWAP_V3_BURN_TOPIC0,
                ADDRESS_50_TOPIC,
                WORD_NEG_10,
                WORD_POS_10,
            ],
            UNISWAP_BURN_DATA,
        );

        registry.decode_log_in_place(&mut swap);
        registry.decode_log_in_place(&mut mint);
        registry.decode_log_in_place(&mut burn);

        assert_eq!(swap.decoded_event.as_ref().unwrap()["event"], "Swap");
        assert_eq!(
            swap.decoded_event.as_ref().unwrap()["args"]["amount0"],
            "-5"
        );
        assert_eq!(swap.decoded_event.as_ref().unwrap()["args"]["tick"], "-120");
        assert_eq!(mint.decoded_event.as_ref().unwrap()["event"], "Mint");
        assert_eq!(
            mint.decoded_event.as_ref().unwrap()["args"]["sender"],
            hex_bytes(&[0x40; 20])
        );
        assert_eq!(burn.decoded_event.as_ref().unwrap()["event"], "Burn");
        assert_eq!(burn.decoded_event.as_ref().unwrap()["args"]["amount1"], "7");
    }

    fn registry<const N: usize>(
        entries: [([u8; 20], AbiStandard, &'static str); N],
    ) -> AbiRegistry {
        AbiRegistry::new(
            entries
                .into_iter()
                .map(|(address, standard, version)| AbiRegistryEntry {
                    address,
                    standard,
                    decoder_version: version.to_owned(),
                }),
        )
        .unwrap()
    }

    fn fixture_log(address: [u8; 20], topics: &[&str], data: &str) -> RawLog {
        RawLog {
            transaction_index: 0,
            log_index: 0,
            tx_hash: [0x44; 32],
            address,
            topics: topics.iter().map(|topic| hex_word(topic)).collect(),
            data: hex_data(data),
            decoded_event: None,
            decoder_version: None,
        }
    }

    fn hex_word(value: &str) -> [u8; 32] {
        hex_data_array(value)
    }

    fn hex_data(value: &str) -> Vec<u8> {
        assert!(value.starts_with("0x"));
        let value = &value[2..];
        assert_eq!(value.len() % 2, 0);
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|digits| u8::from_str_radix(std::str::from_utf8(digits).unwrap(), 16).unwrap())
            .collect()
    }

    fn hex_data_array<const N: usize>(value: &str) -> [u8; N] {
        hex_data(value).try_into().unwrap()
    }
}
