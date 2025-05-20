use crate::common::database::{spawn_database, test_table_name};
use crate::common::pipeline::{spawn_pg_pipeline, PipelineMode};
use async_trait::async_trait;
use pg_replicate::conversions::cdc_event::CdcEvent;
use pg_replicate::conversions::table_row::TableRow;
use pg_replicate::conversions::Cell;
use pg_replicate::pipeline::sinks::{BatchSink, InfallibleSinkError};
use pg_replicate::pipeline::PipelineResumptionState;
use postgres::schema::{ColumnSchema, TableId, TableName, TableSchema};
use postgres::tokio::test_utils::PgDatabase;
use std::collections::{HashMap, HashSet};
use tokio_postgres::types::{PgLsn, Type};

struct TestSink {
    table_schemas: HashMap<TableId, TableSchema>,
    table_rows: HashMap<TableId, Vec<TableRow>>,
    events: Vec<CdcEvent>,
    tables_copied: u8,
    tables_truncated: u8,
}

impl TestSink {
    fn new() -> Self {
        Self {
            table_schemas: HashMap::new(),
            table_rows: HashMap::new(),
            events: Vec::new(),
            tables_copied: 0,
            tables_truncated: 0,
        }
    }
}

#[async_trait]
impl BatchSink for TestSink {
    type Error = InfallibleSinkError;

    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        Ok(PipelineResumptionState {
            copied_tables: HashSet::new(),
            last_lsn: PgLsn::from(0),
        })
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error> {
        self.table_schemas = table_schemas;
        Ok(())
    }

    async fn write_table_rows(
        &mut self,
        rows: Vec<TableRow>,
        table_id: TableId,
    ) -> Result<(), Self::Error> {
        self.table_rows.entry(table_id).or_default().extend(rows);
        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        self.events.extend(events);
        Ok(PgLsn::from(0))
    }

    async fn table_copied(&mut self, _table_id: TableId) -> Result<(), Self::Error> {
        self.tables_copied += 1;
        Ok(())
    }

    async fn truncate_table(&mut self, _table_id: TableId) -> Result<(), Self::Error> {
        self.tables_truncated += 1;
        Ok(())
    }
}

async fn create_and_fill_users_table(database: &PgDatabase, num_users: usize) -> TableId {
    let table_id = database
        .create_table(test_table_name("users"), &vec![("age", "integer")])
        .await
        .unwrap();

    for i in 0..num_users {
        let age = i as i32 + 1;
        database
            .insert_values(test_table_name("users"), &["age"], &[&age])
            .await
            .unwrap();
    }

    table_id
}

fn assert_table_schema(
    sink: &TestSink,
    table_id: TableId,
    expected_table_name: TableName,
    expected_columns: &[ColumnSchema],
) {
    let table_schema = sink.table_schemas.get(&table_id).unwrap();

    assert_eq!(table_schema.table_id, table_id);
    assert_eq!(table_schema.table_name, expected_table_name);

    let columns = &table_schema.column_schemas;
    assert_eq!(columns.len(), expected_columns.len());

    for (actual, expected) in columns.iter().zip(expected_columns.iter()) {
        assert_eq!(actual.name, expected.name);
        assert_eq!(actual.typ, expected.typ);
        assert_eq!(actual.modifier, expected.modifier);
        assert_eq!(actual.nullable, expected.nullable);
        assert_eq!(actual.primary, expected.primary);
    }
}

fn assert_users_table_schema(sink: &TestSink, users_table_id: TableId) {
    let expected_columns = vec![
        ColumnSchema {
            name: "id".to_string(),
            typ: Type::INT8,
            modifier: -1,
            nullable: false,
            primary: true,
        },
        ColumnSchema {
            name: "age".to_string(),
            typ: Type::INT4,
            modifier: -1,
            nullable: true,
            primary: false,
        },
    ];

    assert_table_schema(
        sink,
        users_table_id,
        test_table_name("users"),
        &expected_columns,
    );
}

fn assert_users_age_sum(sink: &TestSink, users_table_id: TableId, num_users: usize) {
    let mut actual_sum = 0;
    let expected_sum = ((num_users * (num_users + 1)) / 2) as i32;
    let rows = sink.table_rows.get(&users_table_id).unwrap();
    for row in rows {
        if let Cell::I32(age) = &row.values[1] {
            actual_sum += age;
        }
    }

    assert_eq!(actual_sum, expected_sum);
}

/*
Tests to write:
- Insert -> table copy
- Insert -> Update -> table copy
- Insert -> cdc
- Insert -> Update -> cdc
- Insert -> cdc -> Update -> cdc
- Insert -> table copy -> crash while copying -> add new table -> check if new table is in the snapshot
 */

#[tokio::test(flavor = "multi_thread")]
async fn test_simple_table_copy() {
    let database = spawn_database().await;

    // We insert 100 rows.
    let users_table_id = create_and_fill_users_table(&database, 100).await;

    // We create a pipeline that copies the users table.
    let mut pipeline = spawn_pg_pipeline(
        &database.options,
        PipelineMode::CopyTable {
            table_names: vec![test_table_name("users")],
        },
        TestSink::new(),
    )
    .await;
    pipeline.start().await.unwrap();

    assert_users_table_schema(pipeline.sink(), users_table_id);
    assert_users_age_sum(pipeline.sink(), users_table_id, 100);
    assert_eq!(pipeline.sink().tables_copied, 1);
    assert_eq!(pipeline.sink().tables_truncated, 1);
}
