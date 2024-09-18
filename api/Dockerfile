FROM rust:1.79.0-slim-bookworm AS builder
WORKDIR /app
COPY . .
ENV SQLX_OFFLINE=true
RUN cargo build --release -p api

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/api api
COPY api/configuration configuration
ENV APP_ENVIRONMENT=prod
ENTRYPOINT ["./api"]
