# M4 Live Acceptance Run

Operator-run 24-hour live soak test against Ethereum Sepolia.

## Run Identity

- Date: 2026-10-04 to 2026-10-05
- Operator: ubuntu (AWS EC2 ip-172-31-37-42)
- Git commit: `23c533a087fb1427696dde7254988fa763e82e6b`
- Command: `scripts/soak.sh`
- Final verdict: PASS

## Parameters

- Primary WS URL, redacted: `wss://eth-sepolia.g.alchemy.com/v2/redacted`
- Primary HTTP URL, redacted: `https://eth-sepolia.g.alchemy.com/v2/redacted`
- Verifier URL, redacted or none: none
- Expected chain ID: 11155111
- Database target, redacted: `postgresql://ubuntu:redacted@localhost/chainweave_soak_24h_...`
- Poll interval: 12000ms
- Budget window: 60s
- Duration: 86400s (24 hours)
- Warm-up duration: 60s
- RSS bound: stable (well within memory bounds throughout)

## Chain And RPC Identity

- Chain: Ethereum Sepolia (11155111)
- Genesis hash: `0x25a5cc106eea7138acab33231d7160d69cb777ee0c2c553fcddf5138993e6dd9`
- Start height: 11839851 (seeded parent 11839850 before live soak)
- Start hash: seeded before live soak
- End height: 11846449
- End hash: `0xc4884c51232fb3f00843574a40ed6666477c007fba063f5743bb006366f9d4e8`

## Results

- Runtime duration: 86400s
- Reconnect count: 0
- Max lag, blocks: 0
- Max wakeup depth: 0 (never exceeded capacity-1 coalescing channel)
- Unreconciled gap count: 0
- Verifier disagreement count: 0
- Panic count: 0
- Canonical blocks: 6599 (heights 11839851 through 11846449)
- Missing heights: 0
- Checkpoint refs canonical: 1 (`last_height=11846449`, `last_hash=0xc4884c51232fb3f00843574a40ed6666477c007fba063f5743bb006366f9d4e8`)
- Sampled RPC hash comparison: 265 hashes checked (sample step 25), 0 mismatches
- Metrics log artifact: `soak-24h-metrics.log`

## Notes

- RPC/provider incidents: none; zero 429 errors or connection drops over 24h
- Database incidents: none; 0 rollbacks or transaction errors
- Reorgs observed: 0 reorgs exceeding pipeline threshold
- Manual interventions: none; completed full 86400s soak unattended inside tmux
- Follow-up work: M4 acceptance criteria satisfied; ready for subsequent milestones

