use std::{collections::HashMap, error::Error, fs::File};

use arrow_array::{
    builder::{ArrayBuilder, Int32Builder},
    Int32Array, RecordBatch,
};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use pg_replicate::{Attribute, ReplicationClient, RowEvent, Table, TableSchema};
use tokio_postgres::{binary_copy::BinaryCopyOutRow, types::Type};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let repl_client = ReplicationClient::new(
        "localhost".to_string(),
        8080,
        "testdb".to_string(),
        "raminder.singh".to_string(),
    );

    let publication = "table1_pub";
    let postgres_client = repl_client.connect().await?;
    let schemas = ReplicationClient::get_schema(&postgres_client, publication).await?;

    let mut column_builders = create_column_builders(&schemas);

    repl_client
        .get_changes(
            &postgres_client,
            &schemas,
            |event, table_schema| match event {
                RowEvent::Insert(row) => {
                    let mut builders = column_builders
                        .get_mut(&table_schema.table)
                        .expect("no builder found");

                    match row {
                        pg_replicate::Row::CopyOut(row) => {
                            insert_in_col(&table_schema.attributes, &mut builders, &row)
                        }
                    }

                    println!(
                        "Got insert event for table {}.{}",
                        table_schema.table.schema, table_schema.table.name
                    );
                }
                RowEvent::Update => {}
                RowEvent::Delete => {}
            },
        )
        .await?;

    for (table, builders) in column_builders {
        let batch = create_record_batch(builders);
        write_parquet(batch, &format!("./{}.{}.parquet", table.schema, table.name));
    }

    Ok(())
}

fn create_record_batch(builders: Vec<(String, Box<dyn ArrayBuilder>)>) -> RecordBatch {
    let mut cols = vec![];
    for (name, mut builder) in builders {
        cols.push((name, builder.finish()))
    }
    let batch = RecordBatch::try_from_iter(cols).expect("failed to create record batch");
    batch
}

fn write_parquet(batch: RecordBatch, file_name: &str) {
    let file = File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(file_name)
        .unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

    writer.write(&batch).expect("Writing batch");

    writer.close().unwrap();
}

fn insert_in_col(
    attributes: &[Attribute],
    builders: &mut [(String, Box<dyn ArrayBuilder>)],
    row: &BinaryCopyOutRow,
) {
    for (i, attr) in attributes.iter().enumerate() {
        match attr.typ {
            Type::INT4 => {
                let col_builder = builders[i]
                    .1
                    .as_any_mut()
                    .downcast_mut::<Int32Builder>()
                    .expect("builder of incorrect type");
                if attr.nullable {
                    col_builder.append_option(row.get::<Option<i32>>(i));
                } else {
                    col_builder.append_value(row.get::<i32>(i));
                }
            }
            ref t => panic!("type {t:?} not yet supported"),
        };
    }
}

fn create_column_builders(
    schemas: &[TableSchema],
) -> HashMap<Table, Vec<(String, Box<dyn ArrayBuilder>)>> {
    let mut table_to_col_builders = HashMap::new();

    for schema in schemas {
        table_to_col_builders.insert(
            schema.table.clone(),
            create_column_builders_for_table(&schema.attributes),
        );
    }

    table_to_col_builders
}

fn create_column_builders_for_table(
    attributes: &[Attribute],
) -> Vec<(String, Box<dyn ArrayBuilder>)> {
    let mut col_builders: Vec<(String, Box<dyn ArrayBuilder>)> = Vec::new();
    for attr in attributes {
        match attr.typ {
            Type::INT4 => {
                col_builders.push((attr.name.clone(), Box::new(Int32Array::builder(128))))
            }
            _ => panic!("not supported"),
        }
    }
    col_builders
}
