SHELL := /usr/bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.ONESHELL:

POSTGRES_STATE_IMAGE ?= postgres:16-alpine
POSTGRES_STATE_CONTAINER ?= chainweave-postgres-state
POSTGRES_STATE_PORT ?= 55432
POSTGRES_STATE_PROVIDER ?= auto
POSTGRES_STATE_DATA_DIR ?= target/postgres-state-data
POSTGRES_STATE_SOCKET_DIR ?= target/postgres-state-socket
POSTGRES_STATE_LOG ?= target/postgres-state.log
POSTGRES_STATE_DATABASE_URL ?= postgresql://postgres:postgres@127.0.0.1:$(POSTGRES_STATE_PORT)/postgres
M8_LITE_POSTGRES_CONTAINER ?= chainweave-m8-lite-postgres
M8_LITE_POSTGRES_PORT ?= 55438
M8_LITE_POSTGRES_DATA_DIR ?= target/m8-lite-postgres-data
M8_LITE_POSTGRES_SOCKET_DIR ?= target/m8-lite-postgres-socket
M8_LITE_POSTGRES_LOG ?= target/m8-lite-postgres.log
M8_LITE_DATABASE_URL ?= postgresql://postgres:postgres@127.0.0.1:$(M8_LITE_POSTGRES_PORT)/postgres
M8_LITE_EXTERNAL_POSTGRES ?= false
M10_LITE_POSTGRES_PORT ?= 55440
M10_LITE_HTTP_PORT ?= 19100
M10_LITE_DATABASE_URL ?= postgresql://chainweave:chainweave@127.0.0.1:$(M10_LITE_POSTGRES_PORT)/chainweave

BACKFILL_ACCEPTANCE_RPC_URL ?= https://rpc.sepolia.ethpandaops.io
BACKFILL_ACCEPTANCE_REFERENCE_RPC_URL ?= https://sepolia.gateway.tenderly.co
BACKFILL_ACCEPTANCE_FROM_BLOCK ?= 11594001
BACKFILL_ACCEPTANCE_TO_BLOCK ?= 11644000
BACKFILL_ACCEPTANCE_CONTRACT ?= 0xfff9976782d46cc05630d1f6ebab18b2324d6b14
BACKFILL_ACCEPTANCE_CHAIN_ID ?= 11155111
BACKFILL_ACCEPTANCE_GENESIS_HASH ?= 0x25a5cc106eea7138acab33231d7160d69cb777ee0c2c553fcddf5138993e6dd9
BACKFILL_ACCEPTANCE_WORKERS ?= 8
BACKFILL_ACCEPTANCE_INITIAL_LOG_BLOCKS ?= 100
BACKFILL_ACCEPTANCE_MIN_LOG_BLOCKS ?= 25
BACKFILL_ACCEPTANCE_MAX_LOG_BLOCKS ?= 100
BACKFILL_ACCEPTANCE_RPC_TIMEOUT_MS ?= 30000
BACKFILL_ACCEPTANCE_RPC_MAX_REQUESTS ?= 160000
BACKFILL_ACCEPTANCE_RPC_MAX_COST_UNITS ?= 300000
LIVE_ACCEPTANCE_PRIMARY_WS_URL ?=
LIVE_ACCEPTANCE_VERIFIER_URL ?=
LIVE_ACCEPTANCE_EXPECTED_CHAIN_ID ?=
LIVE_ACCEPTANCE_POLL_INTERVAL_MS ?= 12000
LIVE_ACCEPTANCE_BUDGET_WINDOW_SECS ?= 60
LIVE_ACCEPTANCE_DURATION_SECS ?= 86400
LIVE_ACCEPTANCE_WARMUP_SECS ?= 300
LIVE_ACCEPTANCE_RSS_BOUND_MB ?= 512

.PHONY: test-postgres-state
test-postgres-state:
	if ! cargo sqlx --version >/dev/null 2>&1; then
		printf 'cargo-sqlx is required. Install with:\n'
		printf '  cargo install sqlx-cli --version 0.8.6 --no-default-features --features rustls,postgres --locked\n'
		exit 127
	fi
	backend=
	if [[ '$(POSTGRES_STATE_PROVIDER)' != 'local' ]] && docker ps >/dev/null 2>&1; then
		backend=docker
		if docker ps -a --format '{{.Names}}' | grep -qx '$(POSTGRES_STATE_CONTAINER)'; then
			docker rm -f '$(POSTGRES_STATE_CONTAINER)' >/dev/null
		fi
		docker run -d --rm \
			--name '$(POSTGRES_STATE_CONTAINER)' \
			-e POSTGRES_PASSWORD=postgres \
			-e POSTGRES_DB=postgres \
			-p 127.0.0.1:$(POSTGRES_STATE_PORT):5432 \
			'$(POSTGRES_STATE_IMAGE)' >/dev/null
	else
		if [[ '$(POSTGRES_STATE_PROVIDER)' == 'docker' ]]; then
			printf 'Docker provider requested but Docker is not accessible\n'
			exit 1
		fi
		backend=local
		for command in initdb pg_ctl pg_isready; do
			if ! command -v "$$command" >/dev/null 2>&1; then
				printf 'Neither Docker nor local Postgres command %s is available\n' "$$command"
				exit 1
			fi
		done
		rm -rf '$(POSTGRES_STATE_DATA_DIR)' '$(POSTGRES_STATE_SOCKET_DIR)'
		mkdir -p target '$(POSTGRES_STATE_SOCKET_DIR)'
		initdb -D '$(POSTGRES_STATE_DATA_DIR)' -A trust -U postgres --no-instructions >/dev/null
		pg_ctl -D '$(POSTGRES_STATE_DATA_DIR)' -l '$(POSTGRES_STATE_LOG)' -o "-h 127.0.0.1 -p $(POSTGRES_STATE_PORT) -c unix_socket_directories='$(abspath $(POSTGRES_STATE_SOCKET_DIR))'" start >/dev/null
	fi
	cleanup() {
		if [[ "$$backend" == 'docker' ]]; then
			docker rm -f '$(POSTGRES_STATE_CONTAINER)' >/dev/null 2>&1 || true
		elif [[ "$$backend" == 'local' ]]; then
			pg_ctl -D '$(POSTGRES_STATE_DATA_DIR)' -m fast stop >/dev/null 2>&1 || true
		fi
	}
	trap cleanup EXIT
	for attempt in $$(seq 1 60); do
		if [[ "$$backend" == 'docker' ]]; then
			if docker exec '$(POSTGRES_STATE_CONTAINER)' pg_isready -U postgres >/dev/null 2>&1; then
				break
			fi
		else
			if pg_isready -h 127.0.0.1 -p '$(POSTGRES_STATE_PORT)' -U postgres >/dev/null 2>&1; then
				break
			fi
		fi
		if [[ "$$attempt" == 60 ]]; then
			printf 'Postgres did not become ready in time\n'
			exit 1
		fi
		sleep 1
	done
	CHAINWEAVE_REQUIRE_POSTGRES_TESTS=1 \
	CHAINWEAVE_TEST_DATABASE_URL='$(POSTGRES_STATE_DATABASE_URL)' \
	SQLX_OFFLINE=true \
	cargo test --workspace --all-targets
	DATABASE_URL='$(POSTGRES_STATE_DATABASE_URL)' cargo sqlx migrate run --source migrations
	DATABASE_URL='$(POSTGRES_STATE_DATABASE_URL)' cargo sqlx prepare --workspace --check

.PHONY: test-kafka-outbox
test-kafka-outbox:
	backend=
	if [[ '$(POSTGRES_STATE_PROVIDER)' != 'local' ]] && docker ps >/dev/null 2>&1; then
		backend=docker
		if docker ps -a --format '{{.Names}}' | grep -qx '$(POSTGRES_STATE_CONTAINER)'; then
			docker rm -f '$(POSTGRES_STATE_CONTAINER)' >/dev/null
		fi
		docker run -d --rm \
			--name '$(POSTGRES_STATE_CONTAINER)' \
			-e POSTGRES_PASSWORD=postgres \
			-e POSTGRES_DB=postgres \
			-p 127.0.0.1:$(POSTGRES_STATE_PORT):5432 \
			'$(POSTGRES_STATE_IMAGE)' >/dev/null
	else
		if [[ '$(POSTGRES_STATE_PROVIDER)' == 'docker' ]]; then
			printf 'Docker provider requested but Docker is not accessible\n'
			exit 1
		fi
		backend=local
		for command in initdb pg_ctl pg_isready; do
			if ! command -v "$$command" >/dev/null 2>&1; then
				printf 'Neither Docker nor local Postgres command %s is available\n' "$$command"
				exit 1
			fi
		done
		rm -rf '$(POSTGRES_STATE_DATA_DIR)' '$(POSTGRES_STATE_SOCKET_DIR)'
		mkdir -p target '$(POSTGRES_STATE_SOCKET_DIR)'
		initdb -D '$(POSTGRES_STATE_DATA_DIR)' -A trust -U postgres --no-instructions >/dev/null
		pg_ctl -D '$(POSTGRES_STATE_DATA_DIR)' -l '$(POSTGRES_STATE_LOG)' -o "-h 127.0.0.1 -p $(POSTGRES_STATE_PORT) -c unix_socket_directories='$(abspath $(POSTGRES_STATE_SOCKET_DIR))'" start >/dev/null
	fi
	cleanup() {
		if [[ "$$backend" == 'docker' ]]; then
			docker rm -f '$(POSTGRES_STATE_CONTAINER)' >/dev/null 2>&1 || true
		elif [[ "$$backend" == 'local' ]]; then
			pg_ctl -D '$(POSTGRES_STATE_DATA_DIR)' -m fast stop >/dev/null 2>&1 || true
		fi
	}
	trap cleanup EXIT
	for attempt in $$(seq 1 60); do
		if [[ "$$backend" == 'docker' ]]; then
			if docker exec '$(POSTGRES_STATE_CONTAINER)' pg_isready -U postgres >/dev/null 2>&1; then
				break
			fi
		else
			if pg_isready -h 127.0.0.1 -p '$(POSTGRES_STATE_PORT)' -U postgres >/dev/null 2>&1; then
				break
			fi
		fi
		if [[ "$$attempt" == 60 ]]; then
			printf 'Postgres did not become ready in time\n'
			exit 1
		fi
		sleep 1
	done
	CHAINWEAVE_REQUIRE_POSTGRES_TESTS=1 \
	CHAINWEAVE_TEST_DATABASE_URL='$(POSTGRES_STATE_DATABASE_URL)' \
	SQLX_OFFLINE=true \
	cargo test -p chainweave-sink kafka::tests::rdkafka_mock_cluster_dispatches_and_demo_consumer_deduplicates -- --nocapture

.PHONY: test-anvil-smoke
test-anvil-smoke:
	if ! command -v anvil >/dev/null 2>&1; then
		printf 'anvil is required for the live smoke test\n'
		exit 127
	fi
	backend=
	if [[ '$(POSTGRES_STATE_PROVIDER)' != 'local' ]] && docker ps >/dev/null 2>&1; then
		backend=docker
		if docker ps -a --format '{{.Names}}' | grep -qx '$(POSTGRES_STATE_CONTAINER)'; then
			docker rm -f '$(POSTGRES_STATE_CONTAINER)' >/dev/null
		fi
		docker run -d --rm \
			--name '$(POSTGRES_STATE_CONTAINER)' \
			-e POSTGRES_PASSWORD=postgres \
			-e POSTGRES_DB=postgres \
			-p 127.0.0.1:$(POSTGRES_STATE_PORT):5432 \
			'$(POSTGRES_STATE_IMAGE)' >/dev/null
	else
		if [[ '$(POSTGRES_STATE_PROVIDER)' == 'docker' ]]; then
			printf 'Docker provider requested but Docker is not accessible\n'
			exit 1
		fi
		backend=local
		for command in initdb pg_ctl pg_isready; do
			if ! command -v "$$command" >/dev/null 2>&1; then
				printf 'Neither Docker nor local Postgres command %s is available\n' "$$command"
				exit 1
			fi
		done
		rm -rf '$(POSTGRES_STATE_DATA_DIR)' '$(POSTGRES_STATE_SOCKET_DIR)'
		mkdir -p target '$(POSTGRES_STATE_SOCKET_DIR)'
		initdb -D '$(POSTGRES_STATE_DATA_DIR)' -A trust -U postgres --no-instructions >/dev/null
		pg_ctl -D '$(POSTGRES_STATE_DATA_DIR)' -l '$(POSTGRES_STATE_LOG)' -o "-h 127.0.0.1 -p $(POSTGRES_STATE_PORT) -c unix_socket_directories='$(abspath $(POSTGRES_STATE_SOCKET_DIR))'" start >/dev/null
	fi
	cleanup() {
		if [[ "$$backend" == 'docker' ]]; then
			docker rm -f '$(POSTGRES_STATE_CONTAINER)' >/dev/null 2>&1 || true
		elif [[ "$$backend" == 'local' ]]; then
			pg_ctl -D '$(POSTGRES_STATE_DATA_DIR)' -m fast stop >/dev/null 2>&1 || true
		fi
	}
	trap cleanup EXIT
	for attempt in $$(seq 1 60); do
		if [[ "$$backend" == 'docker' ]]; then
			if docker exec '$(POSTGRES_STATE_CONTAINER)' pg_isready -U postgres >/dev/null 2>&1; then
				break
			fi
		else
			if pg_isready -h 127.0.0.1 -p '$(POSTGRES_STATE_PORT)' -U postgres >/dev/null 2>&1; then
				break
			fi
		fi
		if [[ "$$attempt" == 60 ]]; then
			printf 'Postgres did not become ready in time\n'
			exit 1
		fi
		sleep 1
	done
	CHAINWEAVE_TEST_DATABASE_URL='$(POSTGRES_STATE_DATABASE_URL)' \
	SQLX_OFFLINE=true \
	cargo test -p chainweave-cli --test anvil_live_smoke -- --ignored --nocapture

.PHONY: test-single-anvil-reorg-scenario
test-single-anvil-reorg-scenario:
	if ! command -v anvil >/dev/null 2>&1; then
		printf 'anvil is required for the single Anvil reorg scenario\n'
		exit 127
	fi
	backend=
	if [[ '$(M8_LITE_EXTERNAL_POSTGRES)' == 'true' ]]; then
		backend=external
	elif [[ '$(POSTGRES_STATE_PROVIDER)' != 'local' ]] && docker ps >/dev/null 2>&1; then
		backend=docker
		if docker ps -a --format '{{.Names}}' | grep -qx '$(M8_LITE_POSTGRES_CONTAINER)'; then
			docker rm -f '$(M8_LITE_POSTGRES_CONTAINER)' >/dev/null
		fi
		docker run -d --rm \
			--name '$(M8_LITE_POSTGRES_CONTAINER)' \
			-e POSTGRES_PASSWORD=postgres \
			-e POSTGRES_DB=postgres \
			-p 127.0.0.1:$(M8_LITE_POSTGRES_PORT):5432 \
			'$(POSTGRES_STATE_IMAGE)' >/dev/null
	else
		if [[ '$(POSTGRES_STATE_PROVIDER)' == 'docker' ]]; then
			printf 'Docker provider requested but Docker is not accessible\n'
			exit 1
		fi
		backend=local
		for command in initdb pg_ctl pg_isready; do
			if ! command -v "$$command" >/dev/null 2>&1; then
				printf 'Neither Docker nor local Postgres command %s is available\n' "$$command"
				exit 1
			fi
		done
		rm -rf '$(M8_LITE_POSTGRES_DATA_DIR)' '$(M8_LITE_POSTGRES_SOCKET_DIR)'
		mkdir -p target '$(M8_LITE_POSTGRES_SOCKET_DIR)'
		initdb -D '$(M8_LITE_POSTGRES_DATA_DIR)' -A trust -U postgres --no-instructions >/dev/null
		pg_ctl -D '$(M8_LITE_POSTGRES_DATA_DIR)' -l '$(M8_LITE_POSTGRES_LOG)' -o "-h 127.0.0.1 -p $(M8_LITE_POSTGRES_PORT) -c unix_socket_directories='$(abspath $(M8_LITE_POSTGRES_SOCKET_DIR))'" start >/dev/null
	fi
	cleanup() {
		if [[ "$$backend" == 'docker' ]]; then
			docker rm -f '$(M8_LITE_POSTGRES_CONTAINER)' >/dev/null 2>&1 || true
		elif [[ "$$backend" == 'local' ]]; then
			pg_ctl -D '$(M8_LITE_POSTGRES_DATA_DIR)' -m fast stop >/dev/null 2>&1 || true
		fi
	}
	trap cleanup EXIT
	if [[ "$$backend" != 'external' ]]; then
		for attempt in $$(seq 1 60); do
			if [[ "$$backend" == 'docker' ]]; then
				if docker exec '$(M8_LITE_POSTGRES_CONTAINER)' pg_isready -U postgres >/dev/null 2>&1; then
					break
				fi
			else
				if pg_isready -h 127.0.0.1 -p '$(M8_LITE_POSTGRES_PORT)' -U postgres >/dev/null 2>&1; then
					break
				fi
			fi
			if [[ "$$attempt" == 60 ]]; then
				printf 'Postgres did not become ready in time\n'
				exit 1
			fi
			sleep 1
		done
	fi
	CHAINWEAVE_TEST_DATABASE_URL='$(M8_LITE_DATABASE_URL)' \
	SQLX_OFFLINE=true \
	cargo test -p chainweave-cli --test single_anvil_reorg_scenario -- --ignored --nocapture

.PHONY: test-minimal-packaging-demo
test-minimal-packaging-demo:
	if ! command -v anvil >/dev/null 2>&1; then
		printf 'anvil is required for the minimal packaging demo check\n'
		exit 127
	fi
	if ! command -v curl >/dev/null 2>&1; then
		printf 'curl is required for the minimal packaging demo readiness check\n'
		exit 127
	fi
	docker compose down -v --remove-orphans >/dev/null 2>&1 || true
	cleanup() {
		docker compose down -v --remove-orphans >/dev/null 2>&1 || true
	}
	trap cleanup EXIT
	CHAINWEAVE_DEMO_POSTGRES_PORT='$(M10_LITE_POSTGRES_PORT)' \
	CHAINWEAVE_DEMO_HTTP_PORT='$(M10_LITE_HTTP_PORT)' \
	docker compose up -d --build
	for attempt in $$(seq 1 120); do
		if curl -fsS 'http://127.0.0.1:$(M10_LITE_HTTP_PORT)/ready' >/dev/null 2>&1; then
			break
		fi
		if [[ "$$attempt" == 120 ]]; then
			docker compose ps
			docker compose logs --no-color postgres indexer
			printf 'compose demo did not become ready in time\n'
			exit 1
		fi
		sleep 2
	done
	M8_LITE_EXTERNAL_POSTGRES=true \
	M8_LITE_DATABASE_URL='$(M10_LITE_DATABASE_URL)' \
	$(MAKE) test-single-anvil-reorg-scenario

.PHONY: test-live-acceptance
test-live-acceptance:
	if [[ -z '$(LIVE_ACCEPTANCE_PRIMARY_WS_URL)' ]]; then
		printf 'LIVE_ACCEPTANCE_PRIMARY_WS_URL is required\n'
		exit 2
	fi
	if [[ -z '$(LIVE_ACCEPTANCE_EXPECTED_CHAIN_ID)' ]]; then
		printf 'LIVE_ACCEPTANCE_EXPECTED_CHAIN_ID is required\n'
		exit 2
	fi
	if [[ -z "$${DATABASE_URL:-}" ]]; then
		printf 'DATABASE_URL is required\n'
		exit 2
	fi
	LIVE_ACCEPTANCE_PRIMARY_WS_URL='$(LIVE_ACCEPTANCE_PRIMARY_WS_URL)' \
	LIVE_ACCEPTANCE_VERIFIER_URL='$(LIVE_ACCEPTANCE_VERIFIER_URL)' \
	LIVE_ACCEPTANCE_EXPECTED_CHAIN_ID='$(LIVE_ACCEPTANCE_EXPECTED_CHAIN_ID)' \
	LIVE_ACCEPTANCE_DATABASE_URL="$${DATABASE_URL}" \
	LIVE_ACCEPTANCE_POLL_INTERVAL_MS='$(LIVE_ACCEPTANCE_POLL_INTERVAL_MS)' \
	LIVE_ACCEPTANCE_BUDGET_WINDOW_SECS='$(LIVE_ACCEPTANCE_BUDGET_WINDOW_SECS)' \
	LIVE_ACCEPTANCE_DURATION_SECS='$(LIVE_ACCEPTANCE_DURATION_SECS)' \
	LIVE_ACCEPTANCE_WARMUP_SECS='$(LIVE_ACCEPTANCE_WARMUP_SECS)' \
	LIVE_ACCEPTANCE_RSS_BOUND_MB='$(LIVE_ACCEPTANCE_RSS_BOUND_MB)' \
	SQLX_OFFLINE=true \
	cargo test -p chainweave-cli --test live_acceptance -- --ignored --nocapture

.PHONY: test-backfill-acceptance
test-backfill-acceptance:
	backend=
	if [[ '$(POSTGRES_STATE_PROVIDER)' != 'local' ]] && docker ps >/dev/null 2>&1; then
		backend=docker
		if docker ps -a --format '{{.Names}}' | grep -qx '$(POSTGRES_STATE_CONTAINER)'; then
			docker rm -f '$(POSTGRES_STATE_CONTAINER)' >/dev/null
		fi
		docker run -d --rm \
			--name '$(POSTGRES_STATE_CONTAINER)' \
			-e POSTGRES_PASSWORD=postgres \
			-e POSTGRES_DB=postgres \
			-p 127.0.0.1:$(POSTGRES_STATE_PORT):5432 \
			'$(POSTGRES_STATE_IMAGE)' >/dev/null
	else
		if [[ '$(POSTGRES_STATE_PROVIDER)' == 'docker' ]]; then
			printf 'Docker provider requested but Docker is not accessible\n'
			exit 1
		fi
		backend=local
		for command in initdb pg_ctl pg_isready; do
			if ! command -v "$$command" >/dev/null 2>&1; then
				printf 'Neither Docker nor local Postgres command %s is available\n' "$$command"
				exit 1
			fi
		done
		rm -rf '$(POSTGRES_STATE_DATA_DIR)' '$(POSTGRES_STATE_SOCKET_DIR)'
		mkdir -p target '$(POSTGRES_STATE_SOCKET_DIR)'
		initdb -D '$(POSTGRES_STATE_DATA_DIR)' -A trust -U postgres --no-instructions >/dev/null
		pg_ctl -D '$(POSTGRES_STATE_DATA_DIR)' -l '$(POSTGRES_STATE_LOG)' -o "-h 127.0.0.1 -p $(POSTGRES_STATE_PORT) -c unix_socket_directories='$(abspath $(POSTGRES_STATE_SOCKET_DIR))'" start >/dev/null
	fi
	cleanup() {
		if [[ "$$backend" == 'docker' ]]; then
			docker rm -f '$(POSTGRES_STATE_CONTAINER)' >/dev/null 2>&1 || true
		elif [[ "$$backend" == 'local' ]]; then
			pg_ctl -D '$(POSTGRES_STATE_DATA_DIR)' -m fast stop >/dev/null 2>&1 || true
		fi
	}
	trap cleanup EXIT
	for attempt in $$(seq 1 60); do
		if [[ "$$backend" == 'docker' ]]; then
			if docker exec '$(POSTGRES_STATE_CONTAINER)' pg_isready -U postgres >/dev/null 2>&1; then
				break
			fi
		else
			if pg_isready -h 127.0.0.1 -p '$(POSTGRES_STATE_PORT)' -U postgres >/dev/null 2>&1; then
				break
			fi
		fi
		if [[ "$$attempt" == 60 ]]; then
			printf 'Postgres did not become ready in time\n'
			exit 1
		fi
		sleep 1
	done
	CHAINWEAVE_DATABASE_URL='$(POSTGRES_STATE_DATABASE_URL)' \
	cargo run -p chainweave-cli -- \
		--rpc-url '$(BACKFILL_ACCEPTANCE_RPC_URL)' \
		--expected-chain-id '$(BACKFILL_ACCEPTANCE_CHAIN_ID)' \
		--expected-genesis-hash '$(BACKFILL_ACCEPTANCE_GENESIS_HASH)' \
		backfill \
		--from-block '$(BACKFILL_ACCEPTANCE_FROM_BLOCK)' \
		--to-block '$(BACKFILL_ACCEPTANCE_TO_BLOCK)' \
		--contract-address '$(BACKFILL_ACCEPTANCE_CONTRACT)' \
		--verify-reference \
		--reference-rpc-url '$(BACKFILL_ACCEPTANCE_REFERENCE_RPC_URL)' \
		--initial-log-blocks '$(BACKFILL_ACCEPTANCE_INITIAL_LOG_BLOCKS)' \
		--min-log-blocks '$(BACKFILL_ACCEPTANCE_MIN_LOG_BLOCKS)' \
		--max-log-blocks '$(BACKFILL_ACCEPTANCE_MAX_LOG_BLOCKS)' \
		--rpc-timeout-ms '$(BACKFILL_ACCEPTANCE_RPC_TIMEOUT_MS)' \
		--rpc-max-requests '$(BACKFILL_ACCEPTANCE_RPC_MAX_REQUESTS)' \
		--rpc-max-cost-units '$(BACKFILL_ACCEPTANCE_RPC_MAX_COST_UNITS)' \
		--workers '$(BACKFILL_ACCEPTANCE_WORKERS)'
