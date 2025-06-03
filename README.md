# ETL

> **Note:** Version 2 is currently under development under `/v2`, which includes a complete rework of the pipeline 
> architecture for improved performance and scalability.

A Rust crate to quickly build replication solutions for Postgres. It provides building blocks to construct data pipelines which can continually copy data from Postgres to other systems. It builds abstractions on top of Postgres's [logical streaming replication protocol](https://www.postgresql.org/docs/current/protocol-logical-replication.html) and pushes users towards the pit of success without letting them worry about low level details of the protocol.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [Usage](#usage)
- [Database Setup](#database-setup)
- [Running Tests](#running-tests)
- [Docker](#docker)
- [Architecture](#architecture)
- [Roadmap](#roadmap)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Features

The `etl` crate supports the following destinations:

- [x] BigQuery
- [x] DuckDB
- [x] MotherDuck
- [ ] Snowflake (planned)
- [ ] ClickHouse (planned)
- [ ] Many more to come...

Note: DuckDB and MotherDuck destinations do not use the batched pipeline, hence they currently perform poorly. A batched pipeline version of these destinations is planned.

## Installation

To use `etl` in your Rust project, add it via a git dependency in `Cargo.toml`:

```toml
[dependencies]
etl = { git = "https://github.com/supabase/etl", features = ["stdout"] }
```

Each destination is behind a feature of the same name, so remember to enable the right feature. The git dependency is needed for now because `etl` is not yet published on crates.io.

## Quickstart

To quickly try out `etl`, you can run the `stdout` example, which will replicate the data to standard output. First, create a publication in Postgres which includes the tables you want to replicate:

```sql
create publication my_publication
for table table1, table2;
```

Then run the `stdout` example:

```bash
cargo run -p etl --example stdout --features="stdout" -- --db-host localhost --db-port 5432 --db-name postgres --db-username postgres --db-password password cdc my_publication stdout_slot
```

In the above example, `etl` connects to a Postgres database named `postgres` running on `localhost:5432` with a username `postgres` and password `password`. The slot name `stdout_slot` will be created by `etl` automatically.

## Usage

Here's a basic example of how to use `etl` in your Rust code:

```rust
use std::error::Error;
use etl::pipeline::{
    data_pipeline::DataPipeline,
    destinations::stdout::StdoutDestination,
    sources::postgres::{PostgresSource, TableNamesFrom},
    PipelineAction,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let host = "localhost";
    let port = 5432;
    let database = "postgres";
    let username = "postgres";
    let password = Some("password".to_string());
    let slot_name = Some("my_slot".to_string());
    let table_names = TableNamesFrom::Publication("my_publication".to_string());

    // Create a PostgresSource
    let postgres_source = PostgresSource::new(
        host,
        port,
        database,
        username,
        password,
        slot_name,
        table_names,
    )
    .await?;

    // Create a StdoutDestination
    let stdout_destination = StdoutDestination;

    // Create a `DataPipeline` to connect the source to the destination
    let mut pipeline = DataPipeline::new(postgres_source, stdout_destination, PipelineAction::Both);

    // Start the `DataPipeline` to start copying data from Postgres to stdout
    pipeline.start().await?;

    Ok(())
}
```

For more examples, please refer to the [examples](https://github.com/supabase/etl/tree/main/etl/examples) folder in the source.

## Database Setup

Before running the examples, tests, or the API and replicator components, you'll need to set up a PostgreSQL database.
We provide a convenient script to help you with this setup. For detailed instructions on how to use the database setup script, please refer to our [Database Setup Guide](docs/database-setup.md).

## Running Tests

To run the test suite:

```bash
cargo test
```

For more specific test targets:

```bash
# Run tests for a specific crate
cargo test -p etl

# Run tests with output
cargo test -- --nocapture

# Run a specific test
cargo test test_name
```

## Docker

The repository includes Docker support for both the `replicator` and `api` components:

```bash
# Build replicator image
docker build -f ./replicator/Dockerfile .

# Build api image
docker build -f ./api/Dockerfile .
```

## Architecture

For a detailed explanation of the ETL architecture and design decisions, please refer to our [Design Document](docs/etl-design.md).

## Troubleshooting

### Too Many Open Files Error

If you see the following error when running tests on macOS:

```
called `Result::unwrap()` on an `Err` value: Os { code: 24, kind: Uncategorized, message: "Too many open files" }
```

Raise the limit of open files per process with:

```bash
ulimit -n 10000
```

### Performance Considerations

Currently, the data source and destinations copy table row and CDC events one at a time. This is expected to be slow. Batching and other strategies will likely improve the performance drastically. But at this early stage, the focus is on correctness rather than performance. There are also zero benchmarks at this stage, so commentary about performance is closer to speculation than reality.

## License

Distributed under the Apache-2.0 License. See `LICENSE` for more information.
