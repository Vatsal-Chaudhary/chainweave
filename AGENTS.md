# AGENTS.md — chainweave

Rules for any AI agent (Claude Code, Codex, OpenCode, etc.) working in this repo. The authoritative spec is `docs/reorg-safe-indexer-FDD.md` — read it before touching code, and re-read the relevant milestone section before starting that milestone.

## 1. Source of truth & scope discipline

- `docs/reorg-safe-indexer-FDD.md` is the spec. If code and FDD disagree, stop and flag it — don't silently pick one. If a design decision in the FDD turns out to be wrong once you're implementing it, propose the change and update the FDD in the same PR/commit as the code change. The FDD must never drift out of sync with reality.
- Work one milestone at a time, in order (M0 → M10). Do not start M(n+1) work, scaffolding, or dependencies until M(n)'s acceptance criteria are met and I've confirmed it.
- Do not add scope the FDD didn't ask for (extra chains, extra endpoints, extra crates) without asking first, even if it seems like a natural improvement. Flag it as a suggestion instead of just building it.
- If a milestone looks bigger than it should once you're inside it, say so and propose a split — don't quietly cut corners to make it fit.
- Use FDD milestone labels only when discussing the FDD sequence, acceptance criteria, or project status. Do not copy labels like `M1`/`M2` into commit subjects, migration names, branch names, module/file names, or user-facing feature names; name those after the actual concept being implemented.

## 2. Non-negotiable correctness invariants

These came out of a deliberate design review — do not "simplify" past them:

- **No exactly-once claims.** The delivery model is: idempotent Postgres state + at-least-once Kafka delivery via a transactional outbox + stable event IDs for downstream dedup. Any code or comment implying exactly-once across Postgres+Kafka is a bug in the code, not an acceptable simplification.
- **Never DELETE chain data.** Reorgs tombstone (`is_canonical = false`), they don't remove rows. If you find yourself writing a `DELETE` against blocks or logs (outside of retention/archival policy explicitly scoped later), stop.
- **Ancestry resolution must fall back to RPC.** The in-memory block cache is not assumed to contain full history for every reorg. When a new head's ancestry can't be resolved from the cache, fetch missing parents by hash from the RPC. If ancestry can't be resolved within the configured max depth, fail closed (halt/alert), don't guess.
- **Ordering matters.** Rollbacks are applied descendant-first; applies are ancestor-then-child. Don't reorder this for convenience.
- **Checkpointing is not optional infrastructure to add later.** Every canonical write is checkpointed transactionally, from M2 onward. Backfill and live-streaming code must assume crash recovery is already in place, not something bolted on after.
- **Key logs by `(block_hash, log_index)`, never `(block_number, log_index)`.** This is what makes writes idempotent under reorgs — don't "optimize" it away.

## 3. Before writing any code for a milestone

- Re-read that milestone's spec section and acceptance criteria in the FDD.
- State your implementation plan in plain language first (files/modules touched, key functions, test strategy). Wait for a go-ahead on anything nontrivial — you don't need sign-off for boilerplate, but you do for anything touching the invariants in Section 2.
- Identify what you're NOT implementing yet (things later milestones own) so scope stays contained.

## 4. Testing requirements (non-negotiable, not aspirational)

- M1 (chain buffer / reorg detection) requires unit tests for: simple extension, single-block reorg, deep reorg (5+ blocks), reorg-of-a-reorg, unknown-parent fetch fallback, and max-depth failure. This module needs real coverage, not a smoke test — it's the core of the whole project.
- Any milestone claiming idempotency needs a test that replays the same event stream twice and asserts identical final state — not just "looks right in logs."
- Crash-recovery claims need a real kill-and-restart test (`kill -9` at random points), not a graceful-shutdown-only test.
- Don't mark a milestone "done" or move on if its acceptance criteria in the FDD aren't met by actual passing tests. Tell me plainly if something is partially done rather than rounding up.
