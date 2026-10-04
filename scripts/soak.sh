#!/usr/bin/env bash
set -euo pipefail

# Run a 1-2 hour live soak against a real RPC endpoint using the release binary.
#
# Usage:
#   cargo build --release -p chainweave-cli
#   DATABASE_URL='postgresql://user:pass@host:5432/db' \
#   LIVE_PRIMARY_WS_URL='wss://your-primary-ws' \
#   LIVE_PRIMARY_HTTP_URL='https://your-primary-http' \
#   EXPECTED_CHAIN_ID='11155111' \
#   EXPECTED_GENESIS_HASH='0x25a5cc106eea7138acab33231d7160d69cb777ee0c2c553fcddf5138993e6dd9' \
#   SOAK_START_BLOCK='11830000' \
#   SOAK_DURATION_SECS='7200' \
#   scripts/soak.sh
#
# Optional knobs:
#   SERVER_LISTEN_ADDR        default: 127.0.0.1:9100
#   POLL_INTERVAL_MS         default: 12000
#   RPC_TIMEOUT_MS           default: 30000
#   BUDGET_WINDOW_SECS       default: 60
#   BUDGET_COST_UNITS        default: 1200
#   SHUTDOWN_TIMEOUT_MS      default: 30000
#   METRICS_INTERVAL_SECS    default: 60
#   SOAK_METRICS_LOG         default: target/soak-metrics.log
#   SOAK_SEED_PARENT         default: 1; backfills SOAK_START_BLOCK-1 when DB has no checkpoint

CHAINWEAVE_BIN="${CHAINWEAVE_BIN:-target/release/chainweave}"
SERVER_LISTEN_ADDR="${SERVER_LISTEN_ADDR:-127.0.0.1:9100}"
POLL_INTERVAL_MS="${POLL_INTERVAL_MS:-12000}"
RPC_TIMEOUT_MS="${RPC_TIMEOUT_MS:-30000}"
BUDGET_WINDOW_SECS="${BUDGET_WINDOW_SECS:-60}"
BUDGET_COST_UNITS="${BUDGET_COST_UNITS:-1200}"
SHUTDOWN_TIMEOUT_MS="${SHUTDOWN_TIMEOUT_MS:-30000}"
SHUTDOWN_TIMEOUT_SECS="$(((SHUTDOWN_TIMEOUT_MS + 999) / 1000))"
METRICS_INTERVAL_SECS="${METRICS_INTERVAL_SECS:-60}"
SOAK_METRICS_LOG="${SOAK_METRICS_LOG:-target/soak-metrics.log}"
SOAK_SEED_PARENT="${SOAK_SEED_PARENT:-1}"

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    printf 'required environment variable %s is not set\n' "$name" >&2
    exit 2
  fi
}

require_env DATABASE_URL
require_env LIVE_PRIMARY_WS_URL
require_env EXPECTED_CHAIN_ID
require_env EXPECTED_GENESIS_HASH
require_env SOAK_START_BLOCK

if [[ ! -x "$CHAINWEAVE_BIN" ]]; then
  printf 'release binary %s is missing or not executable; run cargo build --release -p chainweave-cli\n' "$CHAINWEAVE_BIN" >&2
  exit 127
fi

if [[ "$SOAK_SEED_PARENT" == "1" && "$SOAK_START_BLOCK" -gt 0 ]]; then
  require_env LIVE_PRIMARY_HTTP_URL
  if ! command -v psql >/dev/null 2>&1; then
    printf 'psql is required when SOAK_SEED_PARENT=1\n' >&2
    exit 127
  fi

  checkpoint_count="$(
    psql "$DATABASE_URL" -At -v chain_id="$EXPECTED_CHAIN_ID" \
      -c "SELECT count(*) FROM checkpoint WHERE chain_id = :'chain_id'::numeric" 2>/dev/null || printf '0'
  )"
  if [[ "$checkpoint_count" == "0" ]]; then
    parent_block="$((SOAK_START_BLOCK - 1))"
    printf 'seeding parent block %s before live soak\n' "$parent_block"
    env CHAINWEAVE_DATABASE_URL="$DATABASE_URL" \
      "$CHAINWEAVE_BIN" \
        --rpc-url "$LIVE_PRIMARY_HTTP_URL" \
        --expected-chain-id "$EXPECTED_CHAIN_ID" \
        --expected-genesis-hash "$EXPECTED_GENESIS_HASH" \
        backfill \
        --from-block "$parent_block" \
        --to-block "$parent_block" \
        --initial-log-blocks 1 \
        --min-log-blocks 1 \
        --max-log-blocks 1 \
        --rpc-timeout-ms "$RPC_TIMEOUT_MS" \
        --rpc-max-requests 1000 \
        --rpc-max-cost-units 5000 \
        --workers 1
  fi
fi

mkdir -p "$(dirname "$SOAK_METRICS_LOG")"

metrics_loop() {
  local base="http://${SERVER_LISTEN_ADDR}"
  while true; do
    {
      printf '# sampled_at=%s\n' "$(date -Is)"
      printf '## /health\n'
      curl -fsS "${base}/health" || true
      printf '\n## /ready\n'
      curl -fsS "${base}/ready" || true
      printf '\n## /metrics chainweave_live_*\n'
      curl -fsS "${base}/metrics" | grep '^chainweave_live_' || true
      printf '\n'
    } >>"$SOAK_METRICS_LOG" 2>&1
    sleep "$METRICS_INTERVAL_SECS"
  done
}

metrics_loop &
METRICS_PID="$!"
cleanup() {
  kill "$METRICS_PID" >/dev/null 2>&1 || true
  wait "$METRICS_PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

if [[ -n "${SOAK_DURATION_SECS:-}" ]]; then
  set +e
  timeout --signal=TERM --kill-after="${SHUTDOWN_TIMEOUT_SECS}s" "$SOAK_DURATION_SECS" \
    env CHAINWEAVE_DATABASE_URL="$DATABASE_URL" \
    "$CHAINWEAVE_BIN" \
      --rpc-url "$LIVE_PRIMARY_WS_URL" \
      --expected-chain-id "$EXPECTED_CHAIN_ID" \
      --expected-genesis-hash "$EXPECTED_GENESIS_HASH" \
      --server-listen-addr "$SERVER_LISTEN_ADDR" \
      live \
      --start-block "$SOAK_START_BLOCK" \
      --poll-interval-ms "$POLL_INTERVAL_MS" \
      --rpc-timeout-ms "$RPC_TIMEOUT_MS" \
      --budget-window-secs "$BUDGET_WINDOW_SECS" \
      --budget-cost-units "$BUDGET_COST_UNITS" \
      --shutdown-timeout-ms "$SHUTDOWN_TIMEOUT_MS"
  status="$?"
  set -e
  if [[ "$status" -eq 124 ]]; then
    printf 'soak duration elapsed after %s seconds\n' "$SOAK_DURATION_SECS"
    exit 0
  fi
  exit "$status"
else
  env CHAINWEAVE_DATABASE_URL="$DATABASE_URL" \
    "$CHAINWEAVE_BIN" \
      --rpc-url "$LIVE_PRIMARY_WS_URL" \
      --expected-chain-id "$EXPECTED_CHAIN_ID" \
      --expected-genesis-hash "$EXPECTED_GENESIS_HASH" \
      --server-listen-addr "$SERVER_LISTEN_ADDR" \
      live \
      --start-block "$SOAK_START_BLOCK" \
      --poll-interval-ms "$POLL_INTERVAL_MS" \
      --rpc-timeout-ms "$RPC_TIMEOUT_MS" \
      --budget-window-secs "$BUDGET_WINDOW_SECS" \
      --budget-cost-units "$BUDGET_COST_UNITS" \
      --shutdown-timeout-ms "$SHUTDOWN_TIMEOUT_MS"
fi
