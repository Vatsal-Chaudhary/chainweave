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
