FROM rust:1.79.0-slim-bookworm as builder
WORKDIR /app
# TODO: remove protobuf-compiler once the upstream gcp-bigquery-client remove it from its deps
RUN apt update && apt install protobuf-compiler clang -y
COPY . .
RUN cargo build --release -p replicator

FROM rust:1.79.0-slim-bookworm as runtime
WORKDIR /app
COPY --from=builder /app/target/release/replicator replicator
COPY replicator/configuration configuration
ENTRYPOINT ["./replicator"]