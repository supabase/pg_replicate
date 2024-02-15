use std::{
    collections::HashMap,
    error::Error,
    fs::File,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_array::{
    builder::{ArrayBuilder, GenericByteBuilder, Int32Builder, TimestampMicrosecondBuilder},
    types::Utf8Type,
    Int32Array, RecordBatch, TimestampMicrosecondArray,
};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use pg_replicate::{Attribute, ReplicationClient, RowEvent, Table, TableSchema};
use tokio_postgres::{binary_copy::BinaryCopyOutRow, types::Type};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let repl_client = ReplicationClient::new(
        "localhost".to_string(),
        5431,
        "pagila".to_string(),
        "raminder.singh".to_string(),
        "temp_slot".to_string(),
    )
    .await?;

    let publication = "actor_pub";
    let schemas = repl_client.get_schemas(publication).await?;

    let mut column_builders = create_column_builders(&schemas);

    repl_client
        .get_table_snapshot(&schemas, |event, table_schema| match event {
            RowEvent::Insert(row) => {
                let builders = column_builders
                    .get_mut(&table_schema.table)
                    .expect("no builder found");

                match row {
                    pg_replicate::Row::CopyOut(row) => {
                        insert_in_col(&table_schema.attributes, builders, &row)
                    }
                    pg_replicate::Row::Insert(_insert) => {
                        //
                    }
                }
            }
            RowEvent::Update(_update) => {}
            RowEvent::Delete(_delete) => {}
        })
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

    RecordBatch::try_from_iter(cols).expect("failed to create record batch")
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
            Type::VARCHAR => {
                let col_builder = builders[i]
                    .1
                    .as_any_mut()
                    .downcast_mut::<GenericByteBuilder<Utf8Type>>()
                    .expect("builder of incorrect type");
                if attr.nullable {
                    col_builder.append_option(row.get::<Option<&str>>(i));
                } else {
                    col_builder.append_value(row.get::<&str>(i));
                }
            }
            Type::TIMESTAMP => {
                let col_builder = builders[i]
                    .1
                    .as_any_mut()
                    .downcast_mut::<TimestampMicrosecondBuilder>()
                    .expect("builder of incorrect type");
                if attr.nullable {
                    let st = row.get::<Option<SystemTime>>(i).map(|t| {
                        t.duration_since(UNIX_EPOCH)
                            .expect("failed to get duration since unix epoch")
                            .as_micros() as i64
                    });
                    col_builder.append_option(st);
                } else {
                    let st = row.get::<SystemTime>(i);
                    let dur = st
                        .duration_since(UNIX_EPOCH)
                        .expect("failed to get duration since unix epoch")
                        .as_micros() as i64;
                    col_builder.append_value(dur);
                }
            }
            ref t => panic!("type {t:?} not yet supported"),
        };
    }
}

type ColumnBuilders = HashMap<Table, Vec<(String, Box<dyn ArrayBuilder>)>>;

fn create_column_builders(schemas: &[TableSchema]) -> ColumnBuilders {
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
            Type::VARCHAR => col_builders.push((
                attr.name.clone(),
                Box::new(GenericByteBuilder::<Utf8Type>::new()),
            )),
            Type::TIMESTAMP => col_builders.push((
                attr.name.clone(),
                Box::new(TimestampMicrosecondArray::builder(128)),
            )),
            ref t => panic!("type {t:?} not yet supported"),
        }
    }
    col_builders
}
