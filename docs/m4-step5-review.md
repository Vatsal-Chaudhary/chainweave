# M4 Step 5 Review Notes

## Manual Mutation Checks

- Redaction log-label path: changed `live_endpoint_labels` to return the primary URL without `redact_url`. `SQLX_OFFLINE=true cargo test -p chainweave-cli live_runtime::tests::live_endpoint_labels_redact_log_inputs` failed because `path-secret` appeared in the primary label.
- Readiness source of truth: changed `readiness_from_reconnect` to ignore `ReconnectLoop::readiness`. `SQLX_OFFLINE=true cargo test -p chainweave-cli live_runtime::tests::readiness_comes_from_reconnect_loop_state` failed because an unavailable loop reported ready.
- Verifier chain identity: changed `verify_verifier_chain_identity` to return `Ok(())` before checking chain ID. `SQLX_OFFLINE=true cargo test -p chainweave-cli live_runtime::tests::verifier_chain_identity_mismatch_is_rejected` failed because the mismatch was accepted.

`cargo-mutants` was not installed, so these targeted source mutations were used as the Step 5 mutation check.

## Live Runner Path

The live runner now calls `LiveTracker::run` through real RPC/Postgres adapters. Existing-checkpoint startup and reconnect catch-up no longer call `PostgresChainWriter::reconcile_to_head`.
