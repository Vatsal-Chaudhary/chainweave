# syntax=docker/dockerfile:1.7

FROM rust:1.96.0-bookworm AS builder
WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY . .
ENV SQLX_OFFLINE=true
RUN cargo build --release -p chainweave-cli --bin chainweave

FROM rust:1.96.0-bookworm
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/chainweave /usr/local/bin/chainweave
EXPOSE 9100
ENTRYPOINT ["chainweave"]
