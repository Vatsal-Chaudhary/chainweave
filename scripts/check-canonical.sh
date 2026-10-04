#!/usr/bin/env bash
set -euo pipefail

# Check canonical continuity and compare sampled canonical block hashes with RPC.
#
# Usage:
#   DATABASE_URL='postgresql://user:pass@host:5432/db' \
#   LIVE_PRIMARY_HTTP_URL='https://your-primary-http' \
#   EXPECTED_CHAIN_ID='11155111' \
#   scripts/check-canonical.sh
#
# Optional knobs:
#   SAMPLE_STEP   default: 25

SAMPLE_STEP="${SAMPLE_STEP:-25}"

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    printf 'required environment variable %s is not set\n' "$name" >&2
    exit 2
  fi
}

require_env DATABASE_URL
require_env LIVE_PRIMARY_HTTP_URL
require_env EXPECTED_CHAIN_ID

if ! command -v psql >/dev/null 2>&1; then
  printf 'psql is required\n' >&2
  exit 127
fi
if ! command -v curl >/dev/null 2>&1; then
  printf 'curl is required for RPC hash comparison\n' >&2
  exit 127
fi
if ! command -v jq >/dev/null 2>&1; then
  printf 'jq is required for RPC hash comparison\n' >&2
  exit 127
fi

rpc_block_hash() {
  local height="$1"
  local height_hex
  height_hex="$(printf '0x%x' "$height")"
  curl -fsS \
    -H 'content-type: application/json' \
    --data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getBlockByNumber\",\"params\":[\"${height_hex}\",false]}" \
    "$LIVE_PRIMARY_HTTP_URL" | jq -r '.result.hash'
}

psql "$DATABASE_URL" -v chain_id="$EXPECTED_CHAIN_ID" <<'SQL'
\set ON_ERROR_STOP on

WITH canon AS (
  SELECT height, block_hash
  FROM blocks
  WHERE chain_id = :'chain_id'::numeric
    AND is_canonical
),
bounds AS (
  SELECT min(height) AS min_h, max(height) AS max_h, count(*) AS canonical_blocks
  FROM canon
),
expected AS (
  SELECT g.height
  FROM bounds b
  CROSS JOIN LATERAL generate_series(b.min_h, b.max_h) AS g(height)
  WHERE b.min_h IS NOT NULL
),
missing AS (
  SELECT expected.height
  FROM expected
  LEFT JOIN canon USING (height)
  WHERE canon.height IS NULL
),
checkpoint_ok AS (
  SELECT count(*) AS n
  FROM checkpoint c
  JOIN blocks b
    ON b.chain_id = c.chain_id
   AND b.block_hash = c.last_hash
   AND b.height = c.last_height
   AND b.is_canonical
  WHERE c.chain_id = :'chain_id'::numeric
),
first_missing AS (
  SELECT coalesce(
    jsonb_agg(height ORDER BY height),
    '[]'::jsonb
  ) AS heights
  FROM (
    SELECT height
    FROM missing
    ORDER BY height
    LIMIT 20
  ) limited
)
SELECT
  bounds.min_h,
  bounds.max_h,
  bounds.canonical_blocks,
  (SELECT count(*) FROM missing) AS missing_heights,
  (SELECT n FROM checkpoint_ok) AS checkpoint_refs_canonical,
  first_missing.heights AS first_20_missing_heights
FROM bounds
CROSS JOIN first_missing;
SQL

sample_query="
WITH ranked AS (
  SELECT
    height,
    '0x' || encode(block_hash, 'hex') AS block_hash,
    row_number() OVER (ORDER BY height) - 1 AS row_offset
  FROM blocks
  WHERE chain_id = ${EXPECTED_CHAIN_ID}::numeric
    AND is_canonical
),
tip AS (
  SELECT max(height) AS height FROM ranked
)
SELECT ranked.height, ranked.block_hash
FROM ranked, tip
WHERE ranked.row_offset % ${SAMPLE_STEP} = 0
   OR ranked.height = tip.height
ORDER BY ranked.height;
"

mismatches=0
checked=0
while IFS='|' read -r height db_hash; do
  [[ -z "$height" ]] && continue
  rpc_hash="$(rpc_block_hash "$height")"
  checked=$((checked + 1))
  if [[ "${db_hash,,}" != "${rpc_hash,,}" ]]; then
    printf 'hash mismatch height=%s db=%s rpc=%s\n' "$height" "$db_hash" "$rpc_hash" >&2
    mismatches=$((mismatches + 1))
  fi
done < <(psql "$DATABASE_URL" -At -F '|' -c "$sample_query")

printf 'sampled_hashes_checked=%s mismatches=%s sample_step=%s\n' "$checked" "$mismatches" "$SAMPLE_STEP"
if [[ "$mismatches" -ne 0 ]]; then
  exit 1
fi
