# chainweave

A reorg-safe EVM chain indexer in Rust. The project is being delivered incrementally from the [FDD](docs/reorg-safe-indexer-FDD.md); the current baseline establishes validated configuration, Alloy HTTP/WS connectivity, chain identity checks, observability primitives, transactional Postgres state for canonical blocks/logs/checkpoints/outbox rows, and at-least-once Kafka outbox delivery.

## Quickstart

Install the Rust toolchain, then run a local EVM node. Anvil is distributed with Foundry:

```bash
curl -L https://foundry.paradigm.xyz | bash
foundryup
anvil
```

In another shell, read the current head:

```bash
cargo run -p chainweave-cli -- --rpc-url http://127.0.0.1:8545 head
```

The command prints the chain ID, latest block number/hash, and genesis hash as JSON. Pin the expected identity through CLI flags when connecting to a durable environment:

```bash
cargo run -p chainweave-cli -- \
  --rpc-url http://127.0.0.1:8545 \
  --expected-chain-id 31337 \
  --expected-genesis-hash 0xYOUR_32_BYTE_GENESIS_HASH \
  head
```

Configuration precedence is defaults, optional TOML, `CHAINWEAVE_*` environment variables, then CLI flags. Nested environment keys use a double underscore, for example `CHAINWEAVE_RPC__PRIMARY_URL`. Database credentials should be supplied with `CHAINWEAVE_DATABASE_URL`; secrets do not belong in committed TOML files.

## Historical Backfill

Execute a bounded historical backfill range with explicit inclusive block bounds:

```bash
CHAINWEAVE_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres \
cargo run -p chainweave-cli -- \
  --rpc-url http://127.0.0.1:8545 \
  backfill --from-block 100 --to-block 200
```

The command validates worker configuration, requires `database_url`, captures the target head before fetching, verifies any configured chain identity, runs migrations, records or verifies database chain identity, fetches adaptive bounded `eth_getLogs` ranges plus batched canonical blocks by number, validates parent continuity and log block hashes, and commits ranges through the M3 ordered coordinator into the existing M2 transactional Postgres writer.

The M3 acceptance proof is repeatable with the pinned Sepolia WETH range:

```bash
make test-backfill-acceptance
```

The target starts a real Postgres instance, backfills blocks `11594001..=11644000` from `https://rpc.sepolia.ethpandaops.io`, filters contract `0xfff9976782d46cc05630d1f6ebab18b2324d6b14`, fetches an independent normalized reference dataset from `https://sepolia.gateway.tenderly.co`, and compares `(block_hash, tx_hash, log_index, address, topics, data)` records.

Acceptance result from September 6, 2026:

```text
reference comparison: matched 19319 normalized records
backfilled 11594001..=11644000 through height 11644000
committed ranges: 500, blocks: 50000, log records: 19319, elapsed: 442.11s, throughput: 113.09 blocks/s
canonical continuity: 50000 blocks from 11594001 through 11644000
RPC budget used: 51001 requests / 55001 cost units (1020.02 calls per 1k blocks)
```

This remains bounded historical backfill work only. Live streaming, websocket subscriptions, live wakeups, ABI decoding, and Kafka delivery are available in later milestone slices; production metrics are a later milestone.

## Tests

The default suite uses deterministic in-process fixtures and does not depend on a public RPC or local Postgres:

```bash
cargo test --workspace --all-targets
```

When `CHAINWEAVE_TEST_DATABASE_URL` is unset, the Postgres integration tests print an explicit local skip. They do not silently pass in CI: setting `CI=true` or `CHAINWEAVE_REQUIRE_POSTGRES_TESTS=1` makes a missing database URL a test failure.

Use the Postgres proof command for the M2 acceptance suite:

```bash
make test-postgres-state
```

That target starts a real Postgres instance, runs `cargo test --workspace --all-targets` with Postgres tests required, applies the checked-in migrations, and verifies SQLx offline metadata with:

```bash
cargo sqlx prepare --workspace --check
```

Install the SQLx CLI first if needed:

```bash
cargo install sqlx-cli --version 0.8.6 --no-default-features --features rustls,postgres --locked
```

By default the target uses Docker with `postgres:16-alpine` when Docker is accessible; otherwise it falls back to an ephemeral local `initdb`/`pg_ctl` data directory under `target/`. Force one provider with `POSTGRES_STATE_PROVIDER=docker` or `POSTGRES_STATE_PROVIDER=local`.

If port `55432` is already in use, choose another host port:

```bash
POSTGRES_STATE_PORT=55433 make test-postgres-state
```

The real-testnet smoke test is opt-in:

```bash
CHAINWEAVE_TESTNET_RPC_URL=https://YOUR_TESTNET_RPC \
  cargo test -p chainweave-cli --test testnet_smoke -- --ignored --nocapture
```

The `chainweave-sink` crate provides fail-closed `/health` and `/ready` state plus a Prometheus `/metrics` handler. A long-running worker process will bind these in a later increment; the current baseline establishes and tests the server primitive and the durable Postgres writer.

## Kafka Outbox Delivery

`chainweave kafka-dispatch` publishes committed unpublished `outbox_events` rows one at a time in `event_id` order. It marks `published_at` only after Kafka broker acknowledgement, so a crash after acknowledgement can retry the same row with the same stable `event_id`. Kafka keys are chain-scoped for partition ordering; `event_id` is carried in headers and payload for downstream deduplication.

```bash
CHAINWEAVE_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres \
CHAINWEAVE_KAFKA__BROKERS='["127.0.0.1:9092"]' \
cargo run -p chainweave-cli -- kafka-dispatch
```

The demo consumer commits Kafka offsets and prints a deduplicated view by `event_id`:

```bash
CHAINWEAVE_KAFKA__BROKERS='["127.0.0.1:9092"]' \
cargo run -p chainweave-cli -- kafka-demo-consumer
```

## Reorg flow

The current transition module proves ancestry and emits ordered canonicality transitions. The Postgres writer persists those transitions atomically into canonical block state, raw logs, checkpoint, and durable outbox rows; the Kafka dispatcher publishes committed outbox rows at least once with stable `event_id`s for downstream deduplication.

```mermaid
flowchart TD
    A[Observed new head] --> B{Parent matches current tip?}
    B -->|yes| C[Apply new head]
    B -->|no| D[Walk replacement ancestry]
    D --> E{Header in recent cache?}
    E -->|yes| F[Validate parent link]
    E -->|no| G[Resolver fetch by hash<br/>RPC fallback]
    G --> F
    F --> H{Reached canonical ancestor?}
    H -->|local or persisted lookup| I[Proven common ancestor]
    H -->|no| D
    H -->|max depth/finality crossed| J[Fail closed]
    I --> K[Rollback orphaned blocks<br/>descendant first]
    K --> L[Apply replacement branch<br/>ancestor first]
    L --> M[Return ChainEvent batch]
    M -. durable sink .-> N[Postgres transaction<br/>blocks/logs/checkpoint]
    N -. outbox delivery .-> O[Kafka outbox dispatcher]
```
