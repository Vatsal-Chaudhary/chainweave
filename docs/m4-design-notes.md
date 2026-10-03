# M4 Design Notes: Real-Time Streaming / Live Tip Tracking

These notes describe the M4 implementation and tests.

## Scope Boundaries

M4 adds live tip tracking and reconciliation only. It must reuse the existing M1 ancestry resolver, M2 Postgres writer/checkpoint/outbox transaction boundary, and M3 ordered backfill coordinator plus RPC retry/timeout/budget vocabulary.

M4 does not add ABI decoding, Kafka dispatch, query APIs, new protocol-specific metrics, or new chain support.

## Existing Code To Reuse

- `crates/chainweave-core/src/chain.rs`: `ChainState`, `AncestryResolver`, `ChainBatch`, ordered rollback/apply events, `max_reorg_depth`, and fail-closed finalized-boundary behavior.
- `crates/chainweave-core/src/backfill.rs`: `OrderedCommitCoordinator`, `FetchedRange`, `RetryPolicy`, `RetryDecision`, `RpcBudget`, `RpcMethod`, and `RpcFailure`.
- `crates/chainweave-sink/src/postgres.rs`: `PostgresChainWriter`, `DurableChainBatch`, `PostgresBackfillCommitter`, `SerializedWriter`, durable checkpoint reads, canonical-header lookup, and atomic `apply_batch`.
- `crates/chainweave-rpc/src/client.rs`: primary RPC client, chain identity checks, block/log conversion to `IndexedBlock`, and block/log hash validation patterns.
- `crates/chainweave-cli/src/main.rs`: current M3 retry wrappers are implementation logic that should be extracted or shared instead of duplicated for M4.

## Next Block Selection

Decision: use WebSocket `newHeads` as a low-latency wakeup, not as the source of truth.

The live tracker keeps the durable Postgres checkpoint as its trusted local position. On startup, reconnect, periodic reconciliation, or a head wakeup, it queries the primary RPC current head and compares that with the last committed checkpoint.

The next normal block to process is:

- no checkpoint: live mode refuses to start unless the operator provides an explicit live start point, such as a future `start_block` config value or a "current head minus N" option; if no start point is configured, return a clear error telling the operator to run backfill first;
- checkpoint `(H, hash)`, current head height `C > H`: height `H + 1`;
- checkpoint height `H`, current head height `C == H`: no extension unless the RPC header at `H` has a different hash, in which case this is a same-height tip reorg;
- checkpoint height `H`, current head height `C < H`: fetch the primary RPC header at `C`; if it matches the canonical block stored at `C`, treat the primary RPC as lagging, make no DB state change, mark readiness degraded, and retry later; if it differs, pass that fetched head through `ChainState`; never reorg on height alone;
- notification for `N <= H` with the same hash: duplicate/reordered wakeup, ignored after reconciliation;
- notification for `N > H + 1`: missed wakeup suspected, healed by explicit range reconciliation from `H + 1..=current_head`.

Missed notifications are detected three ways: a height jump in a received head, the periodic poll seeing `current_head.height > checkpoint.height`, and every WebSocket reconnect querying current head before resubscribing. Healing is always explicit fetch-by-height reconciliation from the durable checkpoint to the current primary head; the tracker never assumes notification delivery was complete.

Decision: support polling as a safety path, but prefer WS when available. For `ws`/`wss` RPC URLs, M4 subscribes to `newHeads` for wakeups and also runs periodic polling. For `http`/`https` URLs and deterministic tests, it uses polling only. This keeps live mode usable with current config while preserving the FDD requirement that WS is the low-latency path.

## Backfill To Live Handoff

M3 captures a target head and commits a continuous canonical range that does not exceed that captured head. M4 startup does not trust an in-memory handoff; it reads the durable checkpoint and canonical table. A database with no checkpoint is not treated as an invitation to stream from genesis.

Startup flow:

1. Validate config with the worker profile, connect primary RPC, read current head, and verify expected chain identity if configured.
2. Run migrations and `ensure_chain_identity`, as backfill does.
3. Read checkpoint. If absent, require an explicit live start point; otherwise fail fast with guidance to run bounded backfill first.
4. Query the current primary RPC head.
5. If checkpoint is present, read the canonical header at `checkpoint.last_height` and use it as the parent anchor.
6. If the checkpoint is behind current head and the missing range is a simple forward fill from the stored tip, run ordered catch-up from `checkpoint.last_height + 1` through current head before enabling WS wakeups.
7. If the checkpoint equals the current head by height/hash, enable wakeups immediately.
8. If the checkpoint height matches but hash differs, if current head is below checkpoint with a different hash at that height, or if the new head's parent does not match the stored tip, enter reorg handling through `ChainState`.

For plain catch-up, M4 will use `OrderedCommitCoordinator<IndexedBlock>` with one-block live ranges by default. Each fetched height is validated against its parent anchor and committed through `PostgresBackfillCommitter`, so the boundary cannot skip a height. If a fetched height's `parent_hash` mismatches the current parent anchor, the primary head reorged during catch-up; discard any uncommitted fetched work and exit to the `ChainState` path from the latest durable checkpoint and the freshly observed head. If the first live notification repeats the backfilled target height, the DB writer's idempotent upsert path and the checkpoint comparison make it a no-op rather than double-processing.

Decision: one-block live ranges are preferred over larger catch-up batches for M4. This is less throughput-oriented than M3 backfill, but it gives a simple per-block transaction boundary and simpler deterministic failure tests. Larger live catch-up ranges can be a later performance improvement if the FDD is updated.

Decision: the ordered coordinator is used only while the next fetched block is a direct child of the durable tip and the catch-up path is a simple extension. Any parent mismatch, same-height hash mismatch, or current-head-below-checkpoint hash mismatch switches to `ChainState`, whose `max_reorg_depth` limit is measured from the stored tip/common-ancestor search rather than from the missing range length.

## Reorg Handling At The Tip

The live tracker uses the existing `ChainState::apply` path whenever the current RPC head is not a simple extension of the stored tip.

The live ancestry resolver will provide:

- `header_by_hash`: fetch missing parent headers from the primary RPC by block hash;
- `canonical_header_by_hash`: lookup persisted canonical blocks by hash in Postgres;
- `canonical_header_at_height`: reuse `PostgresChainWriter::canonical_header_at_height`.

The implementation needs one small RPC addition: fetch a header/block by hash. That must be added to `chainweave-rpc` and charged against the existing `RpcBudget` as `RpcMethod::GetBlockByHash`; it must use the same retry/timeout classification as M3. This is not a new retry layer.

For apply payloads after `ChainState` returns a batch, M4 fetches each apply block and raw logs from the primary RPC and validates the fetched header hash equals the apply event hash before constructing `DurableChainBatch`. Logs are fetched with `eth_getLogs` using the `blockHash` filter, not height ranges, so a reorg cannot mix logs from the wrong block number. If a fetched block/log hash mismatch is observed, treat it as evidence the primary head changed during reconciliation: discard the fetched payload and restart reconciliation from the durable checkpoint. This is not a correctness-stop condition by itself.

Supported maximum reorg depth is `indexer.max_reorg_depth` from config, default `2048`. If common ancestry cannot be proven within that depth, if RPC cannot supply required history, or if the transition crosses the finalized boundary, live tracking fail-closes: stop canonical writes, mark health/readiness unavailable, emit the required alert/metric, and require operator-directed resync. It must not guess, silently skip, or delete chain data.

Decision: a proven reorg transition is committed as one durable writer transaction. That preserves M2's atomic "rollback old branch, apply replacement branch, append outbox, move checkpoint" guarantee. Plain extensions and catch-up heights are one block per transaction.

## Reconnect And Reconciliation

After a WebSocket drop, no in-memory notification state is trusted. The trusted state is:

- durable Postgres checkpoint and canonical blocks;
- current primary RPC view after reconnect;
- existing chain identity row and configured expected chain identity.

Reconnect flow:

1. Mark readiness unavailable while disconnected/reconciling.
2. Reconnect with capped, jittered, unbounded retries; readiness remains unavailable for the whole disconnected/reconciling interval.
3. Query current head after reconnect.
4. Reconcile from the durable checkpoint to current head before accepting new wakeups.
5. Resubscribe to `newHeads`, then mark readiness available.

Reconnect retries are deliberately split from bounded RPC request retry semantics. Ordinary RPC calls use bounded attempt policies and the live time-window budget. The WebSocket connection loop is an operator-facing availability loop: it retries forever with a cap on sleep duration and only halts on correctness errors such as chain identity mismatch, unresolved ancestry, finalized-boundary violation, or configured budget policy deciding that the process must stop.

If the drop occurs mid-reorg or mid-transaction, Postgres remains the recovery boundary. A process crash or task failure before commit leaves the checkpoint unchanged; restart replays from that checkpoint. A crash after commit leaves checkpoint, canonical flags, raw logs, and outbox rows committed together; restart continues from the new checkpoint. In-process task failure closes downstream channels, lets the writer finish or roll back its current transaction, marks health/readiness unavailable, and restarts only for transient RPC/WS failures. Correctness errors such as unresolved ancestry halt live writes.

## Runner And Backpressure

M4 production live tracking ships as sequential reconciliation:

- wakeup channel: capacity `1`, coalescing allowed because notifications are wakeups only;
- startup, poll, and reconnect reconciliation run the live tracker from the durable checkpoint to the current primary head;
- normal forward catch-up commits one block per transaction through the ordered coordinator and Postgres writer;
- proven tip reorgs still commit as one durable writer transaction;
- RPC and Postgres calls provide natural backpressure because the next height is not fetched until the current height is validated and committed or rolled back.

There are no production fetch, coordinate, raw-pass-through, decode, or write stage queues in M4. Stage queues can be revisited only with an FDD update and a production path that exercises the same code as the tests. The wakeup channel may coalesce because periodic/reconnect reconciliation heals missed heads.

Supervision policy: if any runtime task exits before shutdown, even successfully, cancel the live runner and treat the early exit as a failure. Correctness errors stop writes. Transient RPC/WS failures reconcile again from the durable checkpoint with backoff. The writer is the only component allowed to mutate canonical state.

Minimal M4 metrics/health only:

- wakeup depth for the capacity-1 coalescing wakeup channel;
- current lag from primary RPC head to checkpoint;
- reconnect count;
- unreconciled gap count;
- provider disagreement count when a verifier is configured.

No M7 metrics matrix, Kafka metrics, query metrics, or decode-failure metrics are added in M4.

## Optional Secondary Provider

If `rpc.verifier_url` is configured, M4 verifies chain identity, height lag, and a small recent-hash window against the secondary provider. Disagreement raises health/metrics alerts. It does not silently fail over and does not vote on fork choice; the primary RPC remains authoritative for v1.

Decision: the FDD's "pauses if configured" policy is deferred. In M4, verifier disagreement records an alert/metric and degrades readiness only; it does not pause canonical writes and does not add a pause-policy config.

## Crash Safety

Atomic in a single DB transaction:

- each normal live extension block: block row, raw logs keyed by `(chain_id, block_hash, log_index)`, apply outbox row, and checkpoint advance;
- each proven reorg transition: descendant-first rollback flags, ancestor-first replacement block/log upserts, rollback/apply outbox rows, and checkpoint move to the replacement tip or common ancestor;
- each ordered startup/reconnect catch-up height, committed as a one-block range through `PostgresBackfillCommitter`.

Restart resumes by reading the durable checkpoint and reconciling to current primary head. No in-memory queue item, notification, or partially fetched block is trusted after restart.

Decision: live catch-up does not use `PostgresChainWriter::reconcile_to_head` as-is for arbitrary multi-block batches. That helper is useful, but M4's tests should drive one-block ordered catch-up through the existing coordinator so the acceptance proof can show per-height gap freedom and per-block transaction recovery.

## RPC Retry, Timeout, And Budget Reuse

M4 must not create a parallel retry stack. The M3 types `RetryPolicy`, `RetryDecision`, `RpcFailure`, `RpcBudget`, and `RpcMethod` remain the shared retry/budget vocabulary.

Implementation decision: move the M3 async retry helpers currently local to `chainweave-cli/src/main.rs` into a reusable module, likely `chainweave-rpc` or `chainweave-core` depending on dependency direction. Backfill and live should call the same helpers. This is a surgical refactor justified by the FDD's reuse requirement.

Budget decision: M3 backfill keeps its lifetime total cap because it is a bounded job. Live mode uses a time-window budget so a healthy long-running tracker cannot exhaust a lifetime cap and cannot reset its allowance every poll. The default M4 live budget is `1200` cost units per minute, expressed with the existing `RpcBudget` and `RpcMethod` vocabulary. The tracker creates one `RpcBudget` for each one-minute window; every RPC call records the appropriate `RpcMethod`; unused budget expires at the end of the window. Exhausting the live window budget marks readiness degraded/unavailable, stops starting new RPC work, backs off until the window rolls over, then retries from the durable checkpoint. It does not terminate the process unless paired with a correctness error.

Default polling interval decision: poll every `12s` when live tracking is healthy. Polling is still only a reconciliation wakeup; if a poll finds missed blocks, catch-up work must fit inside the current one-minute live budget or wait for the next budget window.

All live RPC calls count against the configured/request budget:

- head reads: block-number/latest/genesis calls;
- block-by-number fetches;
- block-by-hash ancestry fetches;
- log fetches;
- optional verifier checks.

Timeouts and 429/provider-limit handling should classify through the same `RpcFailure` rules as M3. Bounded request retries still use `RetryPolicy`; only the outer reconnect loop has unbounded retries.

## Failure Modes And Tests

| Failure | Detection | Recovery | Test |
| --- | --- | --- | --- |
| Dropped head notification | Poll/reconnect sees RPC head above checkpoint | Fetch every missing height from checkpoint + 1 to current head | Fake head source drops N notifications; final checkpoint has no gaps |
| Duplicate notification | Notification hash/height equals known checkpoint or canonical tip | Ignore after reconciliation | Fake source repeats heads; outbox/canonical state unchanged on duplicate |
| Reordered notification | Notification height is behind checkpoint or older than latest observed wakeup | Treat as wakeup only; reconcile current head | Fake source sends 4, 2, 3, 5; checkpoint ends at 5 once |
| Height jump | New head height > checkpoint + 1 | Range reconciliation | Fake source jumps from 10 to 15; commits 11..=15 in order |
| Current head below checkpoint on same chain | Primary RPC reports height below checkpoint; header at that height matches local canonical | Treat primary as stale/lagging; no rollback or checkpoint change; readiness degraded | Stale-RPC fake returns head 8 while checkpoint is 10 with matching height-8 hash |
| Current head below checkpoint on different fork | Primary RPC reports height below checkpoint; header at that height differs from local canonical | Enter `ChainState`; never reorg on height alone | Fake source returns shorter fork with different hash at current height |
| Head reorgs during catch-up | Fetched block parent does not match current parent anchor | Discard uncommitted fetched work; restart through `ChainState` from durable checkpoint | Fake source injects reorg while committing 11..=15 |
| Block/log hash mismatch | `eth_getLogs` by `blockHash` or attached logs disagree with fetched block hash | Discard payload and re-reconcile from durable checkpoint | Fake RPC returns logs for the old block hash after block payload changes |
| WS disconnect | Subscription stream error/EOF | Reconnect with backoff, query current head, reconcile missing heights | Fake stream disconnects between heads; no lost block |
| Disconnect mid-reorg | Task cancellation while reorg is being fetched/coordinated | Trust checkpoint; redo reorg from DB state after reconnect/restart | Fake reorg plus forced disconnect before writer commit |
| Crash before commit | Process killed or crash gate before `tx.commit()` | Transaction rolls back; checkpoint unchanged; restart replays | Existing crash gate extended to live runner test |
| Crash after commit | Crash gate after `tx.commit()` | Checkpoint/outbox/canonical rows are durable; restart continues | Live crash test compares semantic state to clean run |
| Unknown parent not in cache | `ChainState::parent_for` misses cache/local canonical | Fetch parent by hash through resolver | Fake resolver requires `header_by_hash` to prove ancestor |
| Reorg exceeds max depth | `ChainError::MaxDepthExceeded` | Stop writes; mark unhealthy/not ready | Fake fork deeper than configured depth |
| RPC timeout/rate limit | Shared retry helper classifies timeout/429 | Retry with jittered backoff until attempts/budget exhausted | Fake RPC returns timeout/429 before success |
| Live RPC budget exhausted | Time-window `RpcBudget::record` fails | Mark readiness degraded/unavailable, stop new RPC work, back off until the next one-minute budget window, and retry from durable checkpoint | Low-budget fake source test proves no termination and no partial commit beyond last transaction |
| Provider hash disagreement | Verifier recent hash differs from primary | Alert/degrade; no failover | Primary/verifier fakes disagree on recent height |
| Slow writer/RPC call | Current sequential reconciliation step is still in flight | Natural backpressure; next height is not fetched until current step finishes or shutdown cancels retry/backoff | Slow fake RPC/writer tests |
| Task early exit | Join task returns panic/error/Ok before shutdown | Supervisor cancels the runner and marks unhealthy | Inject early task exit in supervisor test |

## Acceptance Criteria Mapped To Tests

FDD: deterministic tests drop, duplicate, and reorder head notifications without losing a block.

- Add deterministic live tracker tests with an in-memory/fake RPC source and fake notification stream.
- Assert final checkpoint height/hash, canonical continuity, and semantic outbox transition order.
- Assert duplicate/reordered notifications do not create extra apply transitions.
- Assert a stale primary RPC reporting `current_head.height < checkpoint.height` with matching canonical hash causes no rollback or state change, while a different hash enters `ChainState`.

FDD: force reconnects without losing a block.

- Fake WS stream returns EOF/errors at chosen points.
- On reconnect, fake current head is ahead of checkpoint.
- Assert reconciliation fetches and commits every missing height before readiness returns.
- Assert reconnect retries are capped-backoff and unbounded, with readiness unavailable throughout the outage.

FDD: reorg handling at the tip.

- Fake chain: canonical A, replacement B with parent mismatch.
- Assert `ChainState` emits descendant-first rollbacks and ancestor-first applies.
- Assert Postgres final canonical rows/logs/checkpoint match the replacement branch.
- Assert unknown-parent fallback calls fetch-by-hash.
- Assert max-depth failure halts without checkpoint movement.
- Assert a reorg injected during catch-up exits the ordered coordinator path and reprocesses through `ChainState`.

FDD: log fetching remains hash-anchored.

- Fake a block payload and `eth_getLogs` response whose block hashes disagree.
- Assert live mode discards the payload and re-reconciles from the durable checkpoint instead of committing mixed-fork logs or halting permanently.

FDD: sequential runner backpressure and supervised task behavior.

- Stall a slow RPC or writer step and assert health plus wakeup tasks remain responsive.
- Inject task failure or clean early exit and assert the supervisor cancels the runner and leaves DB recoverable from checkpoint.

FDD: live testnet 24h run.

- Add an ignored long-running acceptance command, proposed as `make test-live-acceptance`.
- Inputs: primary WS RPC URL, optional verifier URL, expected chain ID/genesis, database URL, RSS bound, poll interval, budget window, and duration.
- Run for 24h against a public testnet.
- Assert zero unreconciled gaps, zero panics, final checkpoint hash matches the primary RPC canonical hash at that height, wakeup depth never exceeds the capacity-1 coalescing channel, and RSS remains within a stated bound after warm-up.
- Record result in `docs/m4-live-acceptance.md` with date, chain, RPC identity excluding secrets, start/end heights, duration, reconnect count, max lag, max wakeup depth, warm-up RSS and max RSS, unreconciled gap count, panic count, and final verdict.
- State explicitly in that result document that a 24h Sepolia run proves live stability, reconnect/reconciliation behavior, wakeup coalescing, and memory behavior under normal public-testnet conditions; it does not prove deterministic reorg correctness. M8 owns deterministic reorg correctness.

## Ambiguities / Review Items

- The FDD says secondary-provider disagreement "pauses if configured", but current config has no pause policy. Decision: verifier disagreement alerts and degrades readiness only. Phase 2's first commit must update the FDD with one line saying pause policy is deferred, before code relies on that behavior.
- Alloy's `watch_canonical_*_from` API shape must be verified during implementation against the pinned Alloy version. Decision: use explicit reconciliation from checkpoint if that API is unsuitable.
- `PostgresChainWriter::reconcile_to_head` exists, but it can apply many blocks in one transaction. Proposed M4 live catch-up uses one-block ordered coordinator commits instead for clearer crash-safety tests.
