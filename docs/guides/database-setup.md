# Database Setup Guide

This guide explains how to set up and initialize the database using the `init_db.sh` script.

## Local Development Setup

For local development, we use a single PostgreSQL database that serves the API, replicator and all tests. This means:

- One database cluster for everything (API, replicator, and testing)
- The database cluster runs on port 5430 by default
- All components (API and replicator) connect to this same database cluster
- When running tests, they use the same database cluster but with a different database per test

## Prerequisites

Before running the script, ensure you have the following installed:
- PostgreSQL client (`psql`)
- Docker (if you want to run PostgreSQL in a container)
- Rust toolchain
- SQLx CLI (if you plan to run migrations)

To install SQLx CLI, run:
```bash
cargo install --version='~0.7' sqlx-cli --no-default-features --features rustls,postgres
```

## Environment Variables

The script uses the following environment variables (with their default values):

| Variable               | Default   | Description                            |
|------------------------|-----------|----------------------------------------|
| `POSTGRES_USER`        | postgres  | Database username                      |
| `POSTGRES_PASSWORD`    | postgres  | Database password                      |
| `POSTGRES_DB`          | postgres  | Database name                          |
| `POSTGRES_PORT`        | 5430      | Port to run PostgreSQL on              |
| `POSTGRES_HOST`        | localhost | Database host                          |
| `POSTGRES_DATA_VOLUME` | -         | Path for persistent storage (optional) |

## Usage

### Basic Usage

Run the script from the repository directory:

```bash
./scripts/init_db.sh
```

This will:
1. Start a PostgreSQL container (if Docker is available)
2. Wait for the database to be ready
3. Run database migrations (if chosen)

### Advanced Options

#### Skip Docker Container

To skip the Docker container setup (useful if you're already running PostgreSQL locally):

```bash
SKIP_DOCKER=1 ./scripts/init_db.sh
```

#### Skip Migrations

To skip running database migrations:

```bash
SKIP_MIGRATIONS=1 ./scripts/init_db.sh
```

#### Persistent Storage

To specify a custom location for persistent storage:

```bash
POSTGRES_DATA_VOLUME="/path/to/storage" ./scripts/init_db.sh
```
