# FDD: `chainweave` — A Reorg-Safe EVM Chain Indexer (Rust)

**Tagline:** A production-grade, crash-safe blockchain indexer that ingests blocks and logs in real time, correctly handles chain reorganizations, maintains idempotent canonical state in Postgres, and delivers events to Kafka at least once through a transactional outbox with stable event IDs for downstream deduplication.

**Positioning:** This is your Rust-native counterpart to `go-event-pipeline` — same category of problem (streaming ingestion, durable delivery, backpressure), new domain (blockchain data) and new language depth (ownership, async runtime internals, zero-copy decoding). It is deliberately designed to double as (a) a resume systems-engineering piece, and (b) a sellable Upwork service/template.

---

## 1. Goals & Non-Goals

### Goals
- Prove real distributed-systems chops in Rust: concurrency, backpressure, crash recovery, idempotency — not just "calls an RPC and writes rows."
- Solve the *actual hard problem* in chain indexing correctly: reorgs. Most junior/mid indexers either ignore reorgs or handle them with `DELETE WHERE block_number > X`, which is naive and loses audit history. Yours won't.
- Maintain an idempotent Postgres view of canonical chain state and publish every committed chain transition to Kafka at least once via a transactional outbox. Every Kafka message carries a stable `event_id`, so consumers can deduplicate retries.
- Be demoable in an interview in under 5 minutes (CLI + dashboard) and packageable as a reusable Upwork service/template with clearly stated operating assumptions.

### Non-Goals (explicitly out of scope for v1 — call these out in the README as "roadmap")
- Multi-chain ingestion in one process (persist chain identity and design for it, but operate one configured chain in v1)
- Independent fork choice or consensus verification — v1 follows one configured primary RPC's canonical view
- Active-active indexer writers or automatic multi-RPC quorum/failover
- Full historical backfill of a chain since genesis (support arbitrary range backfill, not "index all of Ethereum")
- A general-purpose block explorer UI
- Support for non-EVM chains (Solana etc.) — mention as a separate Phase 2 design, not as a drop-in `ChainEvent` implementation

---

## 2. Core Problem Statement (be able to say this cleanly in an interview)

Blockchain nodes reorganize their view of "the canonical chain" periodically — a block your indexer saw and processed at height N can be **orphaned** minutes later and replaced by a different block at the same height. Any indexer that treats "block N" as a permanent, immutable fact will silently corrupt downstream data (double-counted transfers, phantom events, wrong balances).

A correct indexer must:
1. Track enough recent chain history to **detect** when a reorg has happened (new block's `parent_hash` doesn't match the stored canonical hash at `height - 1`).
2. **Find the common ancestor** between the old and new chain view, fetching unknown parents by hash when the replacement branch is not already cached.
3. **Roll back** (mark orphaned, not delete) all data derived from the orphaned blocks, descendant-first.
4. **Replay** the new canonical chain forward from the common ancestor, ancestor-child-first.
5. Do all of this **idempotently** — a crash mid-reorg-handling must not corrupt state; reprocessing the same block or outbox record must not duplicate canonical rows or create a new delivery identity.
6. Distinguish **unsafe**, **safe**, and **finalized** data so downstream consumers can choose their own risk tolerance (a DEX might want data instantly at the cost of occasional reorgs; an exchange listing/withdrawal system should only trust `finalized`).

---

## 3. Correctness and Trust Model

This section defines what `chainweave` guarantees and what it deliberately trusts. These rules are part of the product contract, not implementation detail.

### RPC authority and chain identity
- V1 follows the canonical chain reported by one configured **primary RPC**; it does not independently execute consensus or choose between forks. A secondary RPC may detect lag or disagreement and raise an alert, but it must not silently switch the canonical view in v1.
- At startup, record and verify both `chain_id` and the genesis block hash. Refuse to write if the configured RPC does not match the database identity.
- Treat height-only observations as hints. Canonical identity is always `(chain_id, block_hash)`, and every range is validated by parent-hash continuity before commit.

### Ancestry resolution and failure bounds
- Keep a configurable recent-header cache (default: 256 headers) keyed by hash with parent pointers. The cache accelerates normal shallow reorgs; it is not the correctness boundary.
- When a new head's parent is unknown, fetch missing headers by hash from the primary RPC and walk backward until reaching a persisted canonical ancestor. Validate each fetched header's hash, height decrement, and parent link.
- Configure `max_reorg_depth` separately from cache size (default: 2,048 blocks). If no common ancestor is found within that depth, if the RPC cannot supply required history, or if the walk crosses the recorded finalized boundary, stop canonical writes, mark the indexer unhealthy, and require operator-directed resync from a trusted height. Never guess or silently delete history.

### Ordering and concurrency
- A single serialized chain coordinator owns canonical transitions for one chain. Backfill workers may fetch concurrently, but only the coordinator commits ordered ranges and advances the checkpoint.
- Emit `Rollback` events from the old tip down to the common ancestor's child, then emit `Apply` events from the common ancestor's child up to the new tip.
- Within a block, logs are ordered by `(transaction_index, log_index)`. Kafka ordering is guaranteed only within a partition, never globally across partitions.

### Finality and delivery guarantees
- Source `safe` and `finalized` from native RPC block tags when the chain supports them, and verify that those hashes are ancestors of the current primary head. For chains without reliable native tags, use explicitly configured depth-based fallbacks and persist `status_source = 'depth'`; never present a depth-derived tier as consensus finality without that qualifier.
- A reorg at or below a natively finalized block is treated as a fatal provider/network integrity violation.
- Postgres canonical state and its checkpoint are atomic and idempotent. The same transaction appends stable outbox rows. The dispatcher publishes those rows to Kafka **at least once**; a crash after publish but before marking an outbox row delivered may cause a retry with the same `event_id`, which consumers must deduplicate.

---

## 4. Architecture

```
                    ┌──────────────────────────┐
                    │   JSON-RPC / WS Client    │  (alloy-rs)
                    │ head tracking + backfill  │
                    └────────────┬──────────────┘
                                 │ headers + raw logs
                                 ▼
                    ┌──────────────────────────┐
                    │ Ancestry Resolver + Chain │  recent header cache,
                    │ Coordinator               │  RPC parent fetch fallback
                    └────────────┬──────────────┘
                                 │ ordered Apply / Rollback batch
                                 ▼
                    ┌──────────────────────────┐
                    │ Decoder / Enrichment      │  raw data retained,
                    │ Pipeline                  │  versioned ABI registry
                    └────────────┬──────────────┘
                                 │
                                 ▼
                    ┌──────────────────────────┐
                    │ Postgres Transaction      │  blocks + logs + checkpoint
                    │ (sqlx, tombstones)         │  + stable outbox events
                    └────────────┬──────────────┘
                                 │ committed outbox rows
                                 ▼
                    ┌──────────────────────────┐
                    │ Kafka Outbox Dispatcher   │  at-least-once publish;
                    │ (rdkafka)                 │  consumer dedup by event_id
                    └──────────────────────────┘

  Cross-cutting: tracing, Prometheus metrics, task supervision,
  bounded mpsc channels, timeouts, and cooperative shutdown.
```

**Key design decision — cache-backed ancestry resolution, not a fixed-memory correctness boundary.** Store recent headers (`hash`, `parent_hash`, `height`) in an in-memory hashmap for fast O(depth) walks. If the replacement branch or ancestor is absent, fetch parents by hash and consult persisted canonical blocks. The resolver is bounded by `max_reorg_depth` and fails closed when it cannot prove a common ancestor.

**Key design decision — never `DELETE`, always tombstone canonical blocks.** Blocks carry `is_canonical` and retain their raw logs permanently. Log canonicality is derived by joining to the owning block, so it cannot drift from block state. Reorg transactions flip old blocks, insert or restore replacement blocks, append outbox transition records, and advance the checkpoint atomically. The outbox also preserves the ordered canonicality-transition history; `is_canonical` alone represents only current state.

**Key design decision — confirmation tiers with provenance.** Every canonical block carries `unsafe`, `safe`, or `finalized` plus a `status_source` of `observed`, `native`, or `depth`. Newly seen unsafe blocks are `observed`; native RPC tags are preferred for tier promotion, while configurable confirmation depths are compatibility fallbacks. Tier promotions are ordered state transitions and are also emitted through the outbox.

**Key design decision — reconcile streams, do not trust notifications as a complete ledger.** WS notifications wake the coordinator, but startup, reconnect, gaps, and periodic checks use Alloy's ordered `watch_*_from`/polling capabilities or explicit height-range reconciliation. Every missing height is fetched and linked before the checkpoint moves.

---

## 5. Tech Stack (with rationale — be ready to justify each choice)

| Concern | Choice | Why / operational note |
|---|---|---|
| Async runtime | `tokio` + `tokio-util` | Standard async I/O runtime; use `CancellationToken` and `TaskTracker` for supervised, cooperative shutdown. RPC concurrency is I/O concurrency, not a claim of CPU parallelism. |
| Ethereum client | `alloy-rs` (not `ethers-rs`) | `ethers-rs` is deprecated in favor of Alloy. Use pubsub for low-latency wakeups and evaluate Alloy's `watch_canonical_*_from` streams for ordered catch-up/reconnect behavior. |
| DB access | `sqlx` | Compile-time checked queries and async Postgres access. Check in offline query metadata and run `cargo sqlx prepare --check` in CI; ensure embedded migrations rebuild when the migrations directory changes. |
| Database | PostgreSQL | Transactional boundary for canonical writes, checkpoint advancement, and outbox insertion. |
| Durable log | `rdkafka` → Kafka | Mature librdkafka client. Kafka is an optional deployment profile; document CMake/native linking and OpenSSL/SASL requirements. Kafka transactions do not make Postgres and Kafka atomic, so use the Postgres outbox. |
| CLI/config | `clap` + `figment` | Layered config: file → env → CLI flags. Deserialize into typed config, then validate URLs, nonzero capacities, depth relationships, secrets, and mutually dependent options before opening sinks. |
| Observability | `tracing` + `tracing-subscriber` + `metrics` (Prometheus exporter) | Structured logs, health/readiness state, metrics, and dashboards — non-negotiable for anything called "production-grade." |
| Reorg test harness | Foundry's `anvil` with `anvil_reorg` | Deterministically triggers reorgs in CI. Pin the Foundry image/version because `anvil_reorg` is an Anvil-specific extension. |
| Integration tests | `testcontainers-rs` / `testcontainers-modules` | Real Postgres, Kafka, and Anvil in CI, with pinned image tags and explicit readiness checks rather than mocks. |

---

## 6. Data Model (Postgres, sketch)

```sql
-- V1 operates one configured chain, but every durable row is namespaced by chain_id.
CREATE TABLE chain_identity (
    chain_id        NUMERIC(78, 0) PRIMARY KEY CHECK (chain_id > 0),
    genesis_hash    BYTEA NOT NULL UNIQUE CHECK (octet_length(genesis_hash) = 32),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Current canonical state plus retained orphan history.
CREATE TABLE blocks (
    chain_id        NUMERIC(78, 0) NOT NULL REFERENCES chain_identity(chain_id),
    block_hash      BYTEA NOT NULL CHECK (octet_length(block_hash) = 32),
    parent_hash     BYTEA NOT NULL CHECK (octet_length(parent_hash) = 32),
    height          BIGINT NOT NULL CHECK (height >= 0),
    timestamp       TIMESTAMPTZ NOT NULL,
    is_canonical    BOOLEAN NOT NULL DEFAULT TRUE,
    status          TEXT NOT NULL CHECK (status IN ('unsafe','safe','finalized')),
    status_source   TEXT NOT NULL CHECK (status_source IN ('observed','native','depth')),
    inserted_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, block_hash),
    UNIQUE (chain_id, block_hash, height)
);
CREATE UNIQUE INDEX uq_blocks_canonical_height
    ON blocks (chain_id, height) WHERE is_canonical;

-- Raw logs are immutable source data. Canonicality is derived by joining blocks.
CREATE TABLE logs (
    chain_id          NUMERIC(78, 0) NOT NULL,
    block_hash        BYTEA NOT NULL CHECK (octet_length(block_hash) = 32),
    block_number      BIGINT NOT NULL CHECK (block_number >= 0),
    transaction_index INT NOT NULL CHECK (transaction_index >= 0),
    log_index         INT NOT NULL CHECK (log_index >= 0),
    tx_hash           BYTEA NOT NULL CHECK (octet_length(tx_hash) = 32),
    address           BYTEA NOT NULL CHECK (octet_length(address) = 20),
    topics            BYTEA[] NOT NULL CHECK (cardinality(topics) BETWEEN 0 AND 4),
    data              BYTEA NOT NULL,
    decoded_event     JSONB,
    decoder_version   TEXT,
    PRIMARY KEY (chain_id, block_hash, log_index),
    FOREIGN KEY (chain_id, block_hash, block_number)
        REFERENCES blocks(chain_id, block_hash, height)
);
CREATE INDEX idx_logs_address_order
    ON logs (chain_id, address, block_number, transaction_index, log_index);
CREATE INDEX idx_logs_topic0_order
    ON logs (chain_id, (topics[1]), block_number, transaction_index, log_index)
    WHERE cardinality(topics) > 0;

-- One committed frontier per configured chain. The referenced block must also be
-- canonical; that invariant is checked and updated inside the writer transaction.
CREATE TABLE checkpoint (
    chain_id        NUMERIC(78, 0) PRIMARY KEY REFERENCES chain_identity(chain_id),
    last_height     BIGINT NOT NULL CHECK (last_height >= 0),
    last_hash       BYTEA NOT NULL CHECK (octet_length(last_hash) = 32),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (chain_id, last_hash, last_height)
        REFERENCES blocks(chain_id, block_hash, height)
);

-- Append-only transition journal and transactional Kafka outbox. event_id is
-- allocated once in the Postgres transaction and reused for every publish retry.
CREATE TABLE outbox_events (
    event_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chain_id        NUMERIC(78, 0) NOT NULL REFERENCES chain_identity(chain_id),
    event_kind      TEXT NOT NULL CHECK (event_kind IN
                       ('apply','rollback','safe','finalized')),
    block_hash      BYTEA NOT NULL CHECK (octet_length(block_hash) = 32),
    block_height    BIGINT NOT NULL CHECK (block_height >= 0),
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,
    FOREIGN KEY (chain_id, block_hash, block_height)
        REFERENCES blocks(chain_id, block_hash, height)
);
CREATE INDEX idx_outbox_unpublished
    ON outbox_events (event_id) WHERE published_at IS NULL;
```

Keying logs by `(chain_id, block_hash, log_index)` makes raw ingestion idempotent under reorgs. Reprocessing the same raw log conflicts with the same key; canonicality comes from `blocks`, while a later re-application of a previously orphaned block creates a new outbox transition with a new stable `event_id`. Decoder output may be updated only under an explicit versioned re-decoding operation.

Canonical event queries join `logs` to `blocks` and filter `blocks.is_canonical`, with ordering by `(block_number, transaction_index, log_index)`. At production scale, partition `blocks`, `logs`, and `outbox_events` by chain/height range and define archive/retention plus autovacuum policy; tombstones are an audit feature, not free storage.

---

## 7. Milestones

Each milestone should ship with tests and a short demo (CLI output / log trace) — treat it exactly like your LoadForge FDD discipline.

### M0 — Scaffolding & RPC Foundation
- Cargo workspace: `chainweave-core`, `chainweave-rpc`, `chainweave-sink`, `chainweave-cli`
- Alloy provider wired up (HTTP + WS), config via `figment` (primary RPC URL, optional verifier RPC, DB URL, optional Kafka brokers, cache size, `max_reorg_depth`, confirmation fallbacks)
- Validate chain identity, URLs, depth relationships, queue capacities, and required secrets before starting workers
- `tracing` + basic Prometheus `/metrics`, `/health`, and `/ready` endpoints stubbed
- **Acceptance:** deterministic Anvil/fixture test prints the current head and rejects a chain ID/genesis mismatch; an opt-in `chainweave head` smoke test reads a real testnet RPC without making public RPC availability a CI requirement.

### M1 — Chain Cache & Ancestry Resolution (the core — do not rush this)
- Pure chain-state transition module plus an ancestry resolver interface
- Recent-header cache (hashmap: hash → `BlockHeader{hash, parent_hash, height}`) with RPC-by-hash fallback for unknown parents and persisted-canonical lookup
- `apply(new_head)` detects continuation vs. duplicate vs. reorg vs. gap; validates parent links and enforces `max_reorg_depth`
- Emits ordered batches: descendant-first `ChainEvent::Rollback(block)`, then ancestor-first `ChainEvent::Apply(block)`
- Add a compact README Mermaid diagram of the tested reorg flow: parent mismatch → cache/RPC ancestry resolution → proven common ancestor → descendant-first rollback → ancestor-first apply. Keep later persistence/outbox stages visually identified as future milestones until they exist.
- **Acceptance:** unit/property tests cover simple extension, duplicate heads, skipped notifications, unknown parents, single/deep reorgs, reorg-of-a-reorg, cache eviction, malformed heights/links, a reorg beyond the configured maximum, and finalized-boundary violation. All valid cases resolve to the correct canonical chain and exact event order; invalid/unprovable cases fail closed. Target >90% coverage for this module. The README diagram matches the behavior and ordering demonstrated by these tests without presenting later milestone functionality as implemented.

### M2 — Transactional Postgres State, Checkpointing & Crash Recovery
- `sqlx` migrations for chain identity, blocks, raw logs, checkpoint, and outbox; check in SQLx offline metadata and verify it in CI
- One serialized writer applies each ordered chain batch in a Postgres transaction: flip/restore canonical blocks, upsert raw logs, append stable outbox events, and advance or rewind the checkpoint atomically
- Startup reconciliation compares the checkpoint `(height, hash)` with the primary RPC, walks back through persisted state when it differs, then fills every missing height before live mode
- Graceful shutdown via cancellation token/task tracking: stop accepting new work, finish or roll back the current DB transaction, stop checkpoint advancement, drain bounded work within a timeout, and leave unpublished outbox rows durable
- Supported proof command: `make test-postgres-state` starts a real Postgres instance through Docker when available or an ephemeral local `initdb`/`pg_ctl` data directory otherwise, requires the DB-backed test path, runs the workspace test suite, applies migrations, and runs `cargo sqlx prepare --workspace --check`. Local no-DB runs may skip Postgres tests only with an explicit skip message; CI or `CHAINWEAVE_REQUIRE_POSTGRES_TESTS=1` must fail if the database URL is missing.
- **Acceptance:** feed the same block/log inputs repeatedly and assert identical canonical DB state. Exercise reorg-of-a-reorg and assert logs derive canonicality correctly. Kill the process mid-transaction and immediately after commit at randomized points, restart, and verify canonical rows, checkpoint, and semantic outbox transition history match a clean run (identity sequences may contain harmless gaps after rollback); no checkpoint may reference a noncanonical block.

### M3 — Historical Backfill Engine
- Command surface: `chainweave backfill --from-block F --to-block T`. The implementation validates inclusive ranges, captures a target head, verifies chain identity, runs migrations, records or verifies database chain identity, fetches adaptive bounded `eth_getLogs` ranges plus batched canonical blocks by number, validates hash-anchored parent continuity, and commits through the ordered backfill coordinator into the existing M2 Postgres writer.
- Range-based backfill (`--from-block` / `--to-block`) with concurrent, bounded fetch workers but a single ordered commit coordinator
- Adaptive `eth_getLogs` range sizing, request timeouts, jittered exponential backoff, explicit handling for 429/provider limit responses, and an RPC request/cost budget
- Anchor each fetched range to block hashes, fetch/validate headers, and verify parent continuity immediately before commit. If a reorg invalidates any block in the range, discard and refetch the affected suffix; never commit a height-only mixed-fork result
- Historical/live boundary contract: historical backfill captures a target head `(height, hash)` and commits one continuous canonical range that does not exceed that captured head. M4 live startup must reconcile from the durable checkpoint/captured boundary to the current RPC head using the same ordered coordinator before enabling live wakeups.
- **Acceptance:** backfill 50k blocks of a pinned testnet contract range, then compare normalized `(block_hash, tx_hash, log_index, address, topics, data)` records against a separately fetched reference dataset rather than comparing counts alone. Inject a reorg and rate-limit errors during concurrent fetch; verify only one continuous canonical range commits and report throughput plus RPC calls per 1k blocks.
  - Proof command: `make test-backfill-acceptance`
  - Pinned proof inputs: Sepolia chain ID `11155111`, genesis hash `0x25a5cc106eea7138acab33231d7160d69cb777ee0c2c553fcddf5138993e6dd9`, blocks `11594001..=11644000`, contract `0xfff9976782d46cc05630d1f6ebab18b2324d6b14`, execution RPC `https://rpc.sepolia.ethpandaops.io`, reference RPC `https://sepolia.gateway.tenderly.co`.
  - Acceptance result from September 6, 2026: reference comparison matched `19319` normalized records; committed `500` ordered ranges, `50000` blocks, and `19319` canonical log records in `442.11s`; throughput `113.09 blocks/s`; canonical continuity `50000` blocks from `11594001` through `11644000`; RPC budget used `51001` requests / `55001` cost units, `1020.02` calls per 1k blocks.

### M4 — Real-Time Streaming (Live Tip Tracking)
- Alloy WS subscription for low-latency head wakeups, with ordered `watch_canonical_*_from`/polling or explicit range reconciliation from the durable checkpoint
- On WS disconnect, reconnect with jittered backoff, query the current head, and fetch every missing height; never assume notifications are complete
- Bounded `mpsc` channels between fetch → coordinate → decode → write stages with documented capacities, blocking policy, timeouts, and supervised task failure behavior
- Optional secondary provider verifies chain identity, height lag, and recent hashes. Disagreement raises health/metrics alerts and degrades readiness; verifier pause policy is deferred, and v1 does not silently fail over or vote on fork choice
- **Acceptance:** deterministic tests drop, duplicate, and reorder head notifications and force reconnects without losing a block. Then run against a live testnet for 24h: zero unreconciled gaps, zero panics, bounded queue depth, and RSS remaining within a stated bound after warm-up.

### M5 — Pluggable ABI Decoding
- Contract registry: address → versioned ABI mapping (config file or DB table), decode retained raw logs into typed JSON using Alloy's ABI tooling
- Bundle decoder definitions for ERC-20 and ERC-721 `Transfer`/`Approval`, plus Uniswap V3 pool `Swap`/`Mint`/`Burn`, with example address-to-ABI registry configuration
- Keep protocol output in the existing versioned `decoded_event` JSONB representation for v1; do not add protocol-specific relational tables or schema migrations
- Unknown ABI/signature and decode failures retain raw data and produce metrics instead of blocking canonical ingestion
- Versioned re-decoding can update `decoded_event` without refetching RPC data and without changing raw log identity
- **Acceptance:** feed pinned ERC-20, ERC-721, and Uniswap V3 raw-log fixtures and assert decoded fields and within-block order. Verify that address/ABI registration resolves standards with overlapping event signatures. Change the ABI version, re-decode, and verify raw topics/data remain unchanged.

### M6 — Kafka Outbox Delivery
- Dispatcher claims committed unpublished outbox rows in `event_id` order, publishes them through `rdkafka`, and records `published_at` only after broker acknowledgement. It does not publish event N+1 until N is acknowledged or classified for operator intervention, preserving source order within the chosen Kafka partition
- Configure durable producer settings (`acks=all`, idempotent producer where supported, bounded local queue, delivery timeout) but retain the stated at-least-once contract across the Postgres/Kafka crash boundary
- Messages carry stable `event_id`, chain identity, transition kind, block identity, ordering fields, schema version, and payload. The demo consumer persists offsets and deduplicates by `event_id`
- Partition by contract address when per-contract order is required. Block-level transition/control events use a documented chain key/partition; no global order is promised across partitions
- **Acceptance:** a demo consumer prints decoded events and rollbacks. Inject failure before publish, after broker acknowledgement but before `published_at`, and during restart; assert no gaps, allow retry duplicates with identical `event_id`, and prove the deduplicated consumer view matches Postgres transition history.

### M7 — Observability & Fault-Injection Hardening
- Prometheus metrics: blocks/sec, RPC calls/retries, current lag, checkpoint height, queue saturation, reorg count/depth, provider disagreement, decode failures, unpublished outbox age/count, and Kafka delivery failures
- Health distinguishes degraded RPC/Kafka from correctness-stop conditions such as unresolved ancestry, chain identity mismatch, or finalized-boundary violation
- Structured traces correlate `chain_id`, block hash/height, reorg ID, DB transaction, and outbox `event_id`; never log RPC credentials, DB passwords, or Kafka secrets
- **Acceptance:** run a fault matrix with at least 10 randomized `kill -9` interruptions plus RPC timeouts, malformed responses, provider disagreement, DB disconnects, queue saturation, and Kafka downtime. After recovery, compare canonical state and deduplicated event history to a clean run; verify alerts and readiness transitions fire for each fault.

### M8 — Deterministic Reorg E2E Harness
- Spin up a pinned Anvil version, deploy a test contract, emit ordered logs, and invoke `anvil_reorg` to replace a branch with conflicting events
- Automated test asserts ancestor discovery, descendant-first rollback, ancestor-first apply, canonical joins, checkpoint movement, and outbox/Kafka retry behavior
- Keep synthetic/property tests as the exhaustive algorithm oracle; Anvil proves integration behavior but is not treated as an independent consensus implementation
- **Acceptance:** one command runs the deterministic reorg scenario in CI and produces a concise trace showing detection, rollback, re-apply, and final canonical query results. Record the same scenario as a short asciinema artifact for the README.

### M9 — Query API
- Minimal Axum API: `/blocks/latest` and `/events?address=&from=&to=&status=` backed by canonical joins and deterministic ordering
- Cursor pagination with bounded page size, parameter/byte-length validation, query timeouts, request body limits, rate limiting, and explicit canonical/finality filters. The first page anchors the cursor to a canonical head hash; later pages reject an invalidated anchor after a reorg rather than mixing snapshots
- Bind to localhost by default. Document TLS termination, authentication/API-key policy, CORS allowlist, proxy trust, and error redaction before exposing it publicly
- **Acceptance:** integration tests cover pagination without gaps/duplicates, invalid ranges/addresses, canonical vs. orphan filtering, unsafe/safe/finalized selection, reorgs between pages, rate limits, and unauthenticated access when auth is enabled.

### M10 — Packaging & Demo
- Docker Compose brings up Postgres, the indexer, and the query API; Kafka is an optional profile so the core Postgres indexer remains easy to run
- Pin images, add readiness checks, run migrations explicitly, use unprivileged containers/read-only filesystems where practical, and keep secrets out of images and checked-in config
- Provide sample validated config, retention guidance, resource limits, graceful stop timeout, and operator runbooks for resync, unresolved deep reorg, provider mismatch, and outbox backlog
- **Acceptance:** `docker compose up` starts a healthy Postgres-only demo; enabling the Kafka profile adds the outbox dispatcher/consumer. Hit the REST API, run the M8 reorg scenario, and observe correct rollback/re-sync without manual database repair.

### Optional M11 — Webhook Outbox Delivery (post-v1)
- Add a webhook dispatcher as a second consumer of committed outbox transitions without changing the Postgres/Kafka at-least-once contract delivered in M6
- Track delivery state per configured destination in dedicated durable rows; do not reuse the Kafka-oriented `published_at` field as shared multi-sink state
- Deliver signed HTTP requests with stable `event_id`, bounded timeouts, jittered exponential backoff, destination-specific ordering, retry limits, and explicit dead-letter/operator recovery
- Validate destination configuration and address SSRF risk, secret rotation, response-size limits, observability, and shutdown behavior. PostgreSQL `LISTEN/NOTIFY` may be used only as a wakeup optimization around durable polling, never as the delivery mechanism or durable queue
- **Acceptance:** integration tests cover success, timeout, retryable and permanent HTTP failures, crash after receiver acknowledgement but before recording delivery, restart, disabled destinations, and dead-letter recovery. Retries retain the same `event_id`; a deduplicating receiver's final transition history matches the durable Postgres outbox for that destination.

---

## 8. Stretch Goals (mention as roadmap, don't block v1 on these)
- Second EVM chain support (prove the namespace and abstraction with separate chain coordinators)
- Automatic multi-RPC failover with health scoring, lag detection, genesis verification, disagreement quarantine, and an explicit authority/quorum policy
- Active-passive leader election for high availability; never permit two uncoordinated canonical writers
- Height-based table partitioning, automated retention/archive policy, and larger-scale performance testing
- gRPC streaming API alongside REST
- GraphQL query API as a client-specific extension after the hardened M9 REST contract; define equivalent query-cost, authorization, pagination, and reorg-snapshot protections before exposing it publicly
- Grafana dashboard JSON checked into the repo (free "looks professional" points)
- A separately designed Solana indexer that models slots and commitment semantics without forcing them into the EVM event model

---

## 9. How This Serves Each of Your Goals

**Resume/interview:** the ancestry resolver (M1), atomic crash recovery/outbox boundary (M2), and deterministic reorg harness (M8) are your "let me walk you through the hardest part" artifacts. Each has explicit invariants and failure tests rather than relying on a happy-path demo.

**Upwork:** package the Postgres path through M5 plus M8–M10 as the default "custom EVM indexer" service; offer Kafka M6 when a client needs downstream fan-out and the optional M11 webhook dispatcher when a client needs direct HTTP delivery. The sellable baseline includes chain identity validation, crash recovery, graceful shutdown, raw log retention, bundled ERC-20/ERC-721/Uniswap V3 decoding templates, protected/paginated queries, and documented resync behavior. Offer GraphQL as separately scoped client customization rather than implying it is part of v1. Lead with reorg safety, but state the primary-RPC trust and at-least-once delivery boundaries precisely.

**Learning:** you'll get real reps on Rust ownership and async state machines (cache-backed ancestry resolution), backpressure and supervised shutdown (bounded stages), transactional boundaries (Postgres state/checkpoint/outbox), and Kafka producer/consumer failure semantics without claiming guarantees the architecture cannot provide.

---

## 10. Suggested Repo Name & README Framing

`chainweave` (or your own naming) — after M1, lead the README with a one-paragraph explanation of the reorg problem (Section 2 above, tightened) and the tested reorg-flow diagram. After M8, add the GIF/asciinema of the Anvil reorg test passing, followed by the architecture diagram and a "quickstart: docker compose up" section when M10 ships. Put a compact correctness table near the top covering RPC trust, maximum supported reorg depth, replay/log ordering, finality source, Postgres atomicity, Kafka at-least-once delivery, and downstream deduplication. Label optional and future capabilities plainly until their milestones pass. That ordering gets a recruiter or client to the impressive artifact without hiding the operating contract.

---

## Changelog

- **Client-facing scope refinement:** Added an M1 README reorg-flow diagram, bundled ERC-20/ERC-721/Uniswap V3 decoder templates in M5 without protocol-specific tables, an optional post-v1 M11 webhook dispatcher with independent durable delivery state, and GraphQL as a separately scoped roadmap extension. Explicitly excluded PostgreSQL notifications as durable delivery.
- **Guarantee correction ([CRITICAL]):** Replaced cross-sink "exactly once" claims with atomic/idempotent Postgres state plus transactional-outbox, at-least-once Kafka delivery and stable `event_id` deduplication.
- **Reorg algorithm correction ([CRITICAL]):** Replaced the fixed 256-header correctness assumption with a cache-backed ancestry resolver, RPC-by-hash parent fetching, persistent lookup, ordered replay, and a fail-closed `max_reorg_depth` policy.
- **Trust/finality contract ([CRITICAL]/[SHOULD FIX]):** Added the primary-RPC trust model, chain identity verification, no-independent-fork-choice boundary, native safe/finalized tags, depth fallback provenance, and finalized-boundary failure behavior.
- **Schema correction ([CRITICAL]):** Added chain/genesis identity, raw topics/data, block and transaction ordering, byte constraints, a unique canonical-height index, canonicality-by-block join, checkpoint references, and the outbox transition journal.
- **Crash-safety sequencing ([CRITICAL]):** Moved checkpointing, startup reconciliation, transactional outbox creation, and graceful shutdown into M2 before backfill and streaming.
- **Backfill/live correctness ([CRITICAL]/[SHOULD FIX]):** Added hash-anchored range validation, reorg invalidation/refetch, a single commit coordinator, deterministic handoff, reconnect reconciliation, bounded queue policy, and fault injection.
- **Kafka semantics ([CRITICAL]/[SHOULD FIX]):** Defined acknowledgement, retry, deduplication, schema, and per-partition ordering guarantees without promising global order or Postgres/Kafka atomicity.
- **Milestone split ([CRITICAL]):** Split the former combined M8 into M8 deterministic reorg E2E, M9 hardened query API, and M10 packaging/demo milestones.
- **Stack/operations updates ([SHOULD FIX]):** Added Alloy catch-up streams, SQLx offline CI checks, `rdkafka` native dependencies, pinned `anvil_reorg`/testcontainers tooling, config validation, security defaults, metrics, retention guidance, and operator runbooks.
