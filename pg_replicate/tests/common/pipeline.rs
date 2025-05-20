use pg_replicate::pipeline::batching::data_pipeline::BatchDataPipeline;
use pg_replicate::pipeline::batching::BatchConfig;
use pg_replicate::pipeline::sinks::BatchSink;
use pg_replicate::pipeline::sources::postgres::{PostgresSource, TableNamesFrom};
use pg_replicate::pipeline::PipelineAction;
use postgres::schema::TableName;
use postgres::tokio::options::PgDatabaseOptions;
use std::time::Duration;

pub enum PipelineMode {
    /// In this mode the supplied tables will be copied.
    CopyTable { table_names: Vec<TableName> },
    /// In this mode the changes will be consumed from the given publication and slot.
    ///
    /// If the slot is not supplied, a new one will be created on the supplied publication.
    Cdc {
        publication: String,
        slot_name: String,
    },
}

pub fn test_slot_name(slot_name: &str) -> String {
    format!("test_{}", slot_name)
}

pub async fn spawn_pg_pipeline<Snk: BatchSink>(
    options: &PgDatabaseOptions,
    mode: PipelineMode,
    sink: Snk,
) -> BatchDataPipeline<PostgresSource, Snk> {
    let batch_config = BatchConfig::new(1000, Duration::from_secs(10));

    let pipeline = match mode {
        PipelineMode::CopyTable { table_names } => {
            let source = PostgresSource::new(
                options.clone(),
                vec![],
                None,
                TableNamesFrom::Vec(table_names),
            )
            .await
            .expect("Failure when creating the Postgres source for copying tables");
            let action = PipelineAction::TableCopiesOnly;
            BatchDataPipeline::new(source, sink, action, batch_config)
        }
        PipelineMode::Cdc {
            publication,
            slot_name,
        } => {
            let source = PostgresSource::new(
                options.clone(),
                vec![],
                Some(test_slot_name(&slot_name)),
                TableNamesFrom::Publication(publication),
            )
            .await
            .expect("Failure when creating the Postgres source for cdc");
            let action = PipelineAction::CdcOnly;
            BatchDataPipeline::new(source, sink, action, batch_config)
        }
    };

    pipeline
}
