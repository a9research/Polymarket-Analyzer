# syntax=docker/dockerfile:1
# Multi-stage: release binary linked against glibc (Debian), TLS via rustls — no OpenSSL in runtime.

FROM rust:1-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --locked --release

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/polymarket-account-analyzer /usr/local/bin/polymarket-account-analyzer

ENV RUST_LOG=info
EXPOSE 3000

# 子命令由 compose / `docker run` 覆盖；默认启动 HTTP API（需 DATABASE_URL 连上 Postgres 才有缓存/raw）
ENTRYPOINT ["/usr/local/bin/polymarket-account-analyzer"]
CMD ["serve", "--bind", "0.0.0.0:3000"]
