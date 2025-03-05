FROM rust:1.85.0-slim-bookworm AS builder
WORKDIR /app
# TODO: remove protobuf-compiler once the upstream gcp-bigquery-client remove it from its deps
RUN apt update && apt install protobuf-compiler clang -y
COPY . .
RUN cargo build --release -p replicator

FROM debian:bookworm-slim
WORKDIR /app
RUN apt update && apt install ca-certificates -y
COPY --from=builder /app/target/release/replicator replicator
COPY replicator/configuration configuration
ENTRYPOINT ["./replicator"]
