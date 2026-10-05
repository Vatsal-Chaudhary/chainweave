# chainweave

A reorg-safe EVM chain indexer in Rust. The project is being delivered incrementally from the [FDD](docs/reorg-safe-indexer-FDD-v2.md); the current baseline establishes validated configuration, Alloy HTTP/WS connectivity, chain identity checks, observability primitives, transactional Postgres state for canonical blocks/logs/checkpoints/outbox rows, and at-least-once Kafka outbox delivery.

## Correctness contract

| Concern | Contract |
|---|---|
| Reorg handling | Detect parent mismatches, prove the common ancestor, roll back orphaned blocks descendant-first, then apply the replacement branch ancestor-first. |
| Chain history | Never delete chain data during reorg handling; orphaned blocks are retained with `is_canonical = false`. |
| Log identity | Raw logs are keyed by `(chain_id, block_hash, log_index)`, so replaying the same fork is idempotent. |
| Postgres state | Canonical block state, raw logs, checkpoints, and outbox rows commit atomically in one transaction. |
| Kafka delivery | Outbox delivery is at least once. Retries reuse stable `event_id` values so downstream consumers can deduplicate. |
| Trust boundary | The indexer follows one configured primary RPC; it does not independently execute consensus or silently fail over to another fork choice. |

## Reorg problem

An EVM block at height N can later be orphaned and replaced by a different block at the same height. An indexer that treats block numbers as permanent facts will keep phantom logs, double-count replacement activity, or corrupt balances. `chainweave` anchors state to block hashes, keeps old fork data for auditability, and updates canonical state only through ordered, idempotent transitions.

## Quickstart

Start the packaged Postgres-only demo:

```bash
docker compose up
```

The compose stack runs pinned Postgres plus a `chainweave` demo process that loads the secret-free sample configuration (`config/chainweave.compose.toml`), receives `CHAINWEAVE_DATABASE_URL` at runtime via `docker-compose.yml`, runs the checked-in migrations explicitly, and exposes:

```text
http://127.0.0.1:19100/health
http://127.0.0.1:19100/ready
http://127.0.0.1:19100/metrics
```

This demo does not ingest blocks by itself. It packages the durable Postgres path so the deterministic Anvil reorg scenario can run against the same database:

```bash
make test-minimal-packaging-demo
```

To run the scenario manually against an already running compose database:

```bash
M8_LITE_EXTERNAL_POSTGRES=true \
M8_LITE_DATABASE_URL=postgresql://chainweave:chainweave@127.0.0.1:55440/chainweave \
make test-single-anvil-reorg-scenario
```

## Local CLI

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

## Live Streaming & 24h Soak Verification

Chainweave tracks the live tip using an Alloy WebSocket subscription for `newHeads` wakeups paired with ordered reconciliation from the durable Postgres checkpoint. To prevent memory runaway during bursts, the subscription feeds a capacity-1 coalescing wakeup channel; on wakeup, the sequential runner reads `latest` and reconciles missing heights sequentially with natural backpressure.

Run live streaming from the durable checkpoint or explicit block height:

```bash
CHAINWEAVE_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres \
cargo run -p chainweave-cli -- \
  --rpc-url wss://eth-sepolia.g.alchemy.com/v2/YOUR_KEY \
  --expected-chain-id 11155111 \
  --expected-genesis-hash 0x25a5cc106eea7138acab33231d7160d69cb777ee0c2c553fcddf5138993e6dd9 \
  --server-listen-addr 127.0.0.1:9100 \
  live \
  --start-block 11839851
```

### Production 24-Hour Soak Acceptance

Streaming stability and memory bounds were verified via an unattended 24-hour soak run (`scripts/soak.sh`) against Ethereum Sepolia on AWS EC2, followed by cryptographic parity validation against primary RPC (`scripts/check-canonical.sh`).

Acceptance result from October 4–5, 2026:

```text
duration: 86400s (24h continuous)
canonical continuity: 6599 blocks from 11839851 through 11846449
missing heights: 0 (zero dropped blocks)
checkpoint refs canonical: 1 (height 11846449, hash 0xc4884c51232fb3f00843574a40ed6666477c007fba063f5743bb006366f9d4e8)
sampled RPC hash verification: 265 blocks checked against Sepolia RPC (step 25), 0 mismatches
live lag blocks: 0
live reconnect count: 0
unreconciled gaps: 0
wakeup channel depth: 0 (never exceeded capacity-1 coalescing limit)
process panics: 0
memory RSS: flat and stable after warm-up
```

Detailed operator log is archived in [`soak-24h-metrics.log`](soak-24h-metrics.log) and [`docs/m4-live-acceptance.md`](docs/m4-live-acceptance.md).

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

## Single Anvil Reorg Scenario

The deterministic M8-lite scenario requires Foundry Anvil `1.7.1` at commit `4072e48705af9d93e3c0f6e29e93b5e9a40caed8`. The test uses Anvil's `anvil_reorg` extension with tuple-style params `[depth, tx_block_pairs]`; replacement transaction block indexes are relative to the replaced branch segment.

Run the scenario with one command:

```bash
make test-single-anvil-reorg-scenario
```

The target starts an isolated disposable Postgres instance, deploys a tiny log-emitting contract on Anvil, writes two old-branch logs, replaces that branch with `anvil_reorg`, reconciles through the existing Postgres writer path, and prints a short trace for detection, rollback, re-apply, and the final canonical log query.

Asciinema artifact: not recorded in this environment because `asciinema` is not installed. To record it locally:

```bash
asciinema rec docs/single-anvil-reorg.cast -c "make test-single-anvil-reorg-scenario"
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

## Scope and roadmap

The packaged demo is intentionally Postgres-only: it validates configuration, applies migrations, exposes health/readiness/metrics, and supports the deterministic Anvil reorg scenario. Kafka remains available through the existing `kafka-dispatch` and demo-consumer commands, but it is not part of the default compose stack.

Deferred work includes a Query API, dashboards, webhooks, operator runbooks, retention guidance, broader deployment hardening, multi-chain operation, and independent fork-choice or consensus verification.
