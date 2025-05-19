use std::collections::HashMap;
use async_trait::async_trait;
use tokio_postgres::types::PgLsn;
use pg_replicate::conversions::cdc_event::CdcEvent;
use pg_replicate::conversions::table_row::TableRow;
use pg_replicate::pipeline::PipelineResumptionState;
use pg_replicate::pipeline::sinks::{BatchSink, InfallibleSinkError};
use postgres::schema::{TableId, TableSchema};
use postgres::tokio::test_utils::PgDatabase;
use crate::common::database::spawn_database;
use crate::common::pipeline::{spawn_pg_pipeline, PipelineMode};
use crate::common::test_table_name;

struct SumSink {
    
}

#[async_trait]
impl BatchSink for SumSink {
    type Error = InfallibleSinkError;

    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        todo!()
    }

    async fn write_table_schemas(&mut self, table_schemas: HashMap<TableId, TableSchema>) -> Result<(), Self::Error> {
        todo!()
    }

    async fn write_table_rows(&mut self, rows: Vec<TableRow>, table_id: TableId) -> Result<(), Self::Error> {
        todo!()
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        todo!()
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        todo!()
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        todo!()
    }
}

async fn create_and_fill_users_table(database: &PgDatabase, num_rows: u32) {
    database.create_table(test_table_name("users"), &vec![("age", "integer")]).await.unwrap();

    for i in 0..num_rows {
        let age = i + 1;
        database.insert_values(
            test_table_name("users"),
            &["age"],
            &[&age],
        ).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_simple_table_copy() {
    let database = spawn_database().await;
    create_and_fill_users_table(&database, 100).await;

    let sink = SumSink {};
    let pipeline = spawn_pg_pipeline(
        &database.options,
        PipelineMode::CopyTable {
            table_names: vec![test_table_name("users")] 
        },
        sink
    );
    
    
}
