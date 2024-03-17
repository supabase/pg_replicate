use apache_avro::{
    to_value,
    types::{Record, Value},
    Reader, Writer,
};
use pg_replicate::TableSchema;
use serde_json::json;

use crate::avro_test;

pub fn write_table_schema(mut writer: Writer<Vec<u8>>, table_schema: &TableSchema) {
    let attributes = table_schema
        .attributes
        .iter()
        .map(|a| avro_test::Attribute {
            identity: a.identity,
            name: a.name.clone(),
            nullable: a.nullable,
            type_modifier: a.type_modifier,
            type_oid: a.typ.oid(),
        })
        .collect();
    let table_schema = avro_test::TableSchema {
        relation_id: table_schema.relation_id,
        schema: table_schema.table.schema.clone(),
        table: table_schema.table.name.clone(),
        attributes,
    };
    // writer
    //     .append_ser(table_schema)
    //     .expect("failed to write table schema");
    let mut record = Record::new(writer.schema()).expect("failed to create record");
    let schema_value = to_value(table_schema).expect("failed to create value");
    let event = Value::Union(0, Box::new(schema_value));
    record.put("event", event);
    writer.append(record).expect("failed to append record");
    let encoded = writer.into_inner().expect("failed to encode");
    println!("ENCODED: LEN: {}, {encoded:?}", encoded.len());
    let reader = Reader::new(&encoded[..]).expect("failed to create reader");
    for value in reader {
        println!("{:?}", value.expect("failed to read value"));
    }
}

// pub fn create_writer(table_schema: &TableSchema) -> Writer<Vec<u8>> {
//     let table_schema_schema_json = create_table_schema_schema_json(table_schema);
//     let table_schema_schema =
//         Schema::parse(&table_schema_schema_json).expect("failed to parse table schema schema");
//     let writer = Writer::new(&table_schema_schema, Vec::new());
//     writer
// }

// The schema so nice I named it twice
pub fn create_table_schema_schema_json(schema: &TableSchema) -> serde_json::Value {
    json!(
        {
          "name": "TableCopyRecord",
          "type": "record",
          "fields": [
            {
              "name": "event",
              "type": [
                {
                  "name": "TableSchema",
                  "type": "record",
                  "fields": [
                    {
                      "name": "relation_id",
                      "type": "int"
                    },
                    {
                      "name": "schema",
                      "type": "string"
                    },
                    {
                      "name": "table",
                      "type": "string"
                    },
                    {
                      "name": "attributes",
                      "type": {
                        "type": "array",
                        "items": {
                          "type": "record",
                          "name": "attribute",
                          "fields": [
                            {
                              "name": "identity",
                              "type": "boolean"
                            },
                            {
                              "name": "name",
                              "type": "string"
                            },
                            {
                              "name": "nullable",
                              "type": "boolean"
                            },
                            {
                              "name": "type_modifier",
                              "type": "int"
                            },
                            {
                              "name": "type_oid",
                              "type": "int"
                            }
                          ]
                        }
                      }
                    }
                  ]
                },
                // {
                //   "name": "InsertRow",
                //   "type": "record",
                //   "fields": [
                //     {
                //       "name": "relation_id",
                //       "type": "int"
                //     },
                //     {
                //       "name": "data",
                //       "type": {
                //         "name": "Data",
                //         "type": "record",
                //         "fields": [
                //           {
                //             "name": "actor_id",
                //             "type": "int"
                //           },
                //           {
                //             "name": "first_name",
                //             "type": "string"
                //           },
                //           {
                //             "name": "last_name",
                //             "type": "string"
                //           }
                //         ]
                //       }
                //     }
                //   ]
                // }
                create_insert_row_schema(schema)
              ]
            }
          ]
        }
    )
}

fn get_type_name(type_oid: u32) -> &'static str {
    match type_oid {
        23 => "int",
        1043 => "string",
        1114 => "string", //string for now, TODO: fix
        oid => panic!("unsupported type oid {oid}"),
    }
}

fn data_record_fields(schema: &TableSchema) -> Vec<serde_json::Value> {
    let mut fields = vec![];
    for attribute in &schema.attributes {
        let name = &attribute.name;
        let type_name = get_type_name(attribute.typ.oid());
        let field = json!({ "name": name, "type": type_name });
        fields.push(field);
    }
    fields
}

fn create_insert_row_schema(schema: &TableSchema) -> serde_json::Value {
    let fields = data_record_fields(schema);
    let insert_row = json!({
        "name": "InsertRow",
        "type": "record",
        "fields": [
            {
                "name": "relation_id",
                "type": "int"
            },
            {
                "name": "data",
                "type": {
                    "name": "Data",
                    "type": "record",
                    "fields": fields
                }
            }
        ]
    });
    insert_row
}
