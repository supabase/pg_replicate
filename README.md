## About `pg_replicate`

`pg_replicate` is a Rust crate to quickly build replication solutions for Postgres. It provides building blocks to construct data pipelines which can continually copy data from Postgres to other systems. It builds abstractions on top of Postgres's [logical streaming replication protocol](https://www.postgresql.org/docs/current/protocol-logical-replication.html) and pushes users towards the pit of success without letting them worry about low level details of the protocol.

## Quickstart

To quickly try out `pg_replicate`, you can run the `stdout` example, which will replicate the data to standard output. First, create a publication in Postgres which includes the tables you want to replicate:

```
create publication my_publication
for table table1, table2;
```

Then run the `stdout` example:

```
cargo run --example stdout -- --db-host localhost --db-port 5432 --db-name postgres --db-username postgres --db-password password cdc my_publication stdout_slot
```

In the above example, `pg_replicate` connects to a Postgres database named `postgres` running on `localhost:5432` with a username `postgres` and password `password`. The slot name `stdout_slot` will be created by `pg_replicate` automatically.

Refer to the [examples](https://github.com/imor/pg_replicate/tree/main/pg_replicate/examples) folder to run examples for sinks other than `stdout` (currently only `bigquery` and `duckdb` supported). A quick tip: to see all the command line options is to just run e.g. `cargo run --example bigquery` which will print the detailed usage instructions for the `bigquery` sink.

## Getting Started

To use `pg_replicate` in your Rust project, add it via a git dependency in `Cargo.toml`:

```toml
[dependencies]
pg_replicate = { git = "https://github.com/imor/pg_replicate" }
```

The git dependency is needed for now because `pg_replicate` is not yet published on crates.io. You'd also need to add a dependency to tokio:

```toml
[dependencies]
...
tokio = { version = "1.38" }
```

Now your `main.rs` can have code like the following:

```rs
use std::error::Error;

use pg_replicate::pipeline::{
    data_pipeline::DataPipeline,
    sinks::stdout::StdoutSink,
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

    // Create a StdoutSink. This sink just prints out the events it receives to stdout
    let stdout_sink = StdoutSink;

    // Create a `DataPipeline` to connect the source to the sink
    let mut pipeline = DataPipeline::new(postgres_source, stdout_sink, PipelineAction::Both);

    // Start the `DataPipeline` to start copying data from Postgres to stdout
    pipeline.start().await?;

    Ok(())
}

```

For more examples, please refer to the [examples](https://github.com/imor/pg_replicate/tree/main/pg_replicate/examples) folder in the source.

## Repository Structure

The repository is a cargo workspace. Each of the individual sub-folders are crate in the workspace. A brief explanation of each crate is as follows:

- `api` - REST api used in hosting `pg_replicate` in a cloud environment.
- `pg_replicate` - The main library crate containing the core logic.
- `queue` - A simple queue for use in Postgres. Again useful for hosting in a cloud environement.
- `replicator` - A binary crate using `pg_replicate`. Packaged as a docker container for use in cloud hosting.

## Roadmap

`pg_replicate` is still under heavy development so expect bugs and papercuts but overtime we plan to add the following sinks.

- [x] Add BigQuery Sink
- [x] Add DuckDb Sink
- [ ] Add Snowflake Sink
- [ ] Add ClickHouse Sink

See the [open issues](https://github.com/imor/pg_replicate/issues) for a full list of proposed features (and known issues).

## License

Distributed under the Apache-2.0 License. See `LICENSE` for more information.

## Docker

To create the docker image for replicator run `docker build -f ./replicator/Dockerfile .` from the root of the repo.

## Design

Applications can use data sources and sinks from `pg_replicate` to build a data pipeline to continually copy data from the source to the sink. For example, a data pipeline to copy data from Postgres to DuckDB takes about 100 lines of Rust.

There are three components in a data pipeline:

1. A data source
2. A data sink
3. A pipline

The data source is an object from where data will be copied. The data sink is an object to which data will be copied. The pipeline is an object which drives the data copy operations from the source to the sink.

```
 +----------+                       +----------+
 |          |                       |          |
 |  Source  |---- Data Pipeline --->|   Sink   |
 |          |                       |          |
 +----------+                       +----------+
```

So roughly you write code like this:

```rust
let postgres_source = PostgresSource::new(...);
let duckdb_sink = DuckDbSink::new(..);
let pipeline = DataPipeline(postgres_source, duckdb_sink);
pipeline.start();
```

Of course, the real code is more than these four lines, but this is the basic idea. For a complete example look at the [duckdb example](https://github.com/imor/pg_replicate/blob/main/pg_replicate/examples/duckdb.rs).

### Data Sources

A data source is the source for data which the pipeline will copy to the data sink. Currently, the repository has only one data source: [`PostgresSource`](https://github.com/imor/pg_replicate/blob/main/pg_replicate/src/pipeline/sources/postgres.rs). `PostgresSource` is the primary data source; data in any other source or sink would have originated from it.

### Data Sinks

A data sink is where the data from a data source is copied. There are two kinds of data sinks. Those which retain the essential nature of data coming out of a `PostgresSource` and those which don't. The former kinds of data sinks can act as a data source in future. The latter kind can't act as a data source and are data's final resting place.

For instance, [`DuckDbSink`](https://github.com/imor/pg_replicate/blob/main/pg_replicate/src/pipeline/sinks/duckdb.rs) ensures that the change data capture (CDC) stream coming in from a source is materialized into tables in a DuckDB database. Once this lossy data transformation is done, it can not be used as a CDC stream again.

Contrast this with a potential future sink `S3Sink` or `KafkaSink` which just copies the CDC stream as is. The data deposited in the sink can later be used as if it was coming from Postgres directly.

### Data Pipeline

A data pipeline encapsulates the business logic to copy the data from the source to the sink. It also orchestrates resumption of the CDC stream from the exact location it was last stopped at. The data sink participates in this by persisting the resumption state and returning it to the pipeline when it restarts.

If a data sink is not transactional (e.g. `S3Sink`), it is not always possible to keep the CDC stream and the resumption state consistent with each other. This can result in these non-transactional sinks having duplicate portions of the CDC stream. Data pipeline helps in deduplicating these duplicate CDC events when the data is being copied over to a transactional store like DuckDB.

Finally, the data pipeline reports back the log sequence number (LSN) upto which the CDC stream has been copied in the sink to the `PostgresSource`. This allows the Postgres database to reclaim disk space by removing WAL segment files which are no longer required by the data sink.

```
 +----------+                       +----------+
 |          |                       |          |
 |  Source  |<---- LSN Numbers -----|   Sink   |
 |          |                       |          |
 +----------+                       +----------+
```

### Kinds of Data Copies

CDC stream is not the only kind of data a data pipeline performs. There's also full table copy, aka backfill. These two kinds can be performed either together or separately. For example, a one-off data copy can use the backfill. But if you want to regularly copy data out of Postgres and into your OLAP database, backfill and CDC stream both should be used. Backfill to get the intial copies of the data and CDC stream to keep those copies up to date and changes in Postgres happen to the copied tables.

### Performance

Currently the data source and sinks copy table row and CDC events one at a time. This is expected to be slow. Batching, and other strategies will likely improve the performance drastically. But at this early stage the focus is on correctness rather than performance. There are also zero benchmarks at this stage, so commentary about performance is closer to speculation than reality.
