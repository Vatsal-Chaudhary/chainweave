CREATE TABLE chain_identity (
    chain_id        NUMERIC(78, 0) PRIMARY KEY CHECK (chain_id > 0),
    genesis_hash    BYTEA NOT NULL UNIQUE CHECK (octet_length(genesis_hash) = 32),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

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

CREATE TABLE checkpoint (
    chain_id        NUMERIC(78, 0) PRIMARY KEY REFERENCES chain_identity(chain_id),
    last_height     BIGINT NOT NULL CHECK (last_height >= 0),
    last_hash       BYTEA NOT NULL CHECK (octet_length(last_hash) = 32),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (chain_id, last_hash, last_height)
        REFERENCES blocks(chain_id, block_hash, height)
);

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
