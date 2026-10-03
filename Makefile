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

BACKFILL_ACCEPTANCE_RPC_URL ?= https://rpc.sepolia.ethpandaops.io
BACKFILL_ACCEPTANCE_REFERENCE_RPC_URL ?= https://sepolia.gateway.tenderly.co
BACKFILL_ACCEPTANCE_FROM_BLOCK ?= 11594001
BACKFILL_ACCEPTANCE_TO_BLOCK ?= 11644000
BACKFILL_ACCEPTANCE_CONTRACT ?= 0xfff9976782d46cc05630d1f6ebab18b2324d6b14
BACKFILL_ACCEPTANCE_CHAIN_ID ?= 11155111
BACKFILL_ACCEPTANCE_GENESIS_HASH ?= 0x25a5cc106eea7138acab33231d7160d69cb777ee0c2c553fcddf5138993e6dd9
BACKFILL_ACCEPTANCE_FETCH_WORKERS ?= 8
BACKFILL_ACCEPTANCE_INITIAL_LOG_BLOCKS ?= 100
BACKFILL_ACCEPTANCE_MIN_LOG_BLOCKS ?= 25
BACKFILL_ACCEPTANCE_MAX_LOG_BLOCKS ?= 100
BACKFILL_ACCEPTANCE_RPC_TIMEOUT_MS ?= 30000
BACKFILL_ACCEPTANCE_RPC_MAX_REQUESTS ?= 160000
BACKFILL_ACCEPTANCE_RPC_MAX_COST_UNITS ?= 300000

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
	CHAINWEAVE_QUEUES__FETCH='$(BACKFILL_ACCEPTANCE_FETCH_WORKERS)' \
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
		--rpc-max-cost-units '$(BACKFILL_ACCEPTANCE_RPC_MAX_COST_UNITS)'
