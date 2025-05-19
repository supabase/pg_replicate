# `pg_replicate` API

This API service provides a RESTful interface for managing PostgreSQL replication pipelines. It enables you to:

- Create and manage replication pipelines between PostgreSQL sources and sinks
- Handle multi-tenant replication configurations
- Manage publications and tables for replication
- Control pipeline lifecycle (start/stop/status)
- Secure configuration with encryption
- Deploy and manage replicators in Kubernetes

## Table of Contents
- [Local Setup](#local-setup)
- [Database Management](#database-management)
- [Development](#development)
- [Environment Variables](#environment-variables)

## Local Setup

### Prerequisites
Before you begin, ensure you have the following installed:
- PostgreSQL client (`psql`)
- SQLx CLI (`cargo install --version='~0.7' sqlx-cli --no-default-features --features rustls,postgres`)
- Rust toolchain

## Database Management

### Initial Setup
To set up and initialize the database, run the following command from the main directory:

```bash
./scripts/init_db.sh
```

This script will:
1. Check for required dependencies (psql and sqlx)
2. Start a PostgreSQL container if one isn't already running
3. Create the database if it doesn't exist
4. Run all migrations

### Environment Variables
You can customize the database setup using these environment variables:

| Variable               | Description                    | Default         |
|------------------------|--------------------------------|-----------------|
| `POSTGRES_DATA_VOLUME` | Data volume path               | ./postgres_data |
| `SKIP_DOCKER`          | Skip Docker container creation | false           |

Example usage:
```bash
POSTGRES_DATA_VOLUME="~/postgres_data" ./scripts/init_db.sh
```

## Development

### Database Migrations

#### Adding a New Migration
To create a new migration file:
```bash
sqlx migrate add <migration-name>
```

#### Running Migrations
To apply all pending migrations:
```bash
sqlx migrate run
```

#### Resetting Database
To reset the database to its initial state:
```bash
sqlx migrate reset
```

#### Updating SQLx Metadata
After making changes to the database schema, update the SQLx metadata:
```bash
cargo sqlx prepare
```