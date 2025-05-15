# Setup

## Preparing the Database

To set up and initialize the database, run the following command from the `api` directory:

```bash
./scripts/init_db.sh
```

This script will:
1. Check for required dependencies (psql and sqlx)
2. Start a PostgreSQL container if one isn't already running
3. Create the database if it doesn't exist
4. Run all migrations

### Prerequisites
- Docker installed and running
- PostgreSQL client (psql) installed
- SQLx CLI installed (if not installed, run: `cargo install --version='~0.7' sqlx-cli --no-default-features --features rustls,postgres`)

### Environment Variables
You can customize the database setup using these environment variables:
- `POSTGRES_USER` (default: postgres)
- `POSTGRES_PASSWORD` (default: postgres)
- `POSTGRES_DB` (default: postgres)
- `POSTGRES_PORT` (default: 5430)
- `POSTGRES_HOST` (default: localhost)
- `POSTGRES_DATA_VOLUME` (default: ./postgres_data)
- `SKIP_DOCKER` (set to any value to skip Docker container creation)

Example:
```bash
POSTGRES_USER=myuser POSTGRES_PASSWORD=mypassword ./scripts/init_db.sh
```

## Adding a new migration

Run the following command from this folder:

```
sqlx migrate add <migration-name>
```

## Running migrations on the local database

Run the following command from this folder:

```
sqlx migrate run
```

## Reset the database to its initial state

Run the following command from this folder:

```
sqlx migrate reset
```

## Update sqlx metadata

Run the following command from this folder:

```
cargo sqlx prepare
```