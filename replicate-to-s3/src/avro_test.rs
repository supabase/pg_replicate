use apache_avro::{Schema, Writer};
use serde::Serialize;

// #[derive]

// const ATTRIBUTE_SCHEMA: &str = r#"
// {
//     "type": "record",
//     "name": "attribute",
//     "fields": [
//         { "name": "identity", "type": "boolean" },
//         { "name": "name", "type": "string" },
//         { "name": "nullable", "type": "boolean" },
//         { "name": "type_modifier", "type": "int" },
//         { "name": "type_oid", "type": "int" }
//     ]
// }
// "#;

// const TABLE_SCHEMA_SCHEMA: &str = r#"
// {
//     "type": "record",
//     "name": "table_schema",
//     "fields": [
//         { "name": "relation_id", "type": "int" },
//         { "name": "schema", "type": "string" },
//         { "name": "table", "type": "string" },
//         { "name": "attributes", "type": { "type": "array", "items": {
//             "type": "record",
//             "name": "attribute",
//             "fields": [
//                 { "name": "identity", "type": "boolean" },
//                 { "name": "name", "type": "string" },
//                 { "name": "nullable", "type": "boolean" },
//                 { "name": "type_modifier", "type": "int" },
//                 { "name": "type_oid", "type": "int" }
//             ]
//         } } }
//     ]
// }
// "#;

#[derive(Serialize)]
pub struct Attribute {
    pub identity: bool,
    pub name: String,
    pub nullable: bool,
    pub type_modifier: i32,
    pub type_oid: u32,
}

#[derive(Serialize)]
pub struct TableSchema {
    pub relation_id: u32,
    pub schema: String,
    pub table: String,
    pub attributes: Vec<Attribute>,
}

// pub fn _write_table_schema(table_schema: &pg_replicate::TableSchema) {
//     // let attribute_schema =
//     //     Schema::parse_str(ATTRIBUTE_SCHEMA).expect("failed to parse attribute schema");
//     let table_schema_schema =
//         Schema::parse_str(TABLE_SCHEMA_SCHEMA).expect("failed to parse table schema");
//     let mut writer = Writer::new(&table_schema_schema, Vec::new());
//     let attributes = table_schema
//         .attributes
//         .iter()
//         .map(|a| Attribute {
//             identity: a.identity,
//             name: a.name.clone(),
//             nullable: a.nullable,
//             type_modifier: a.type_modifier,
//             type_oid: a.typ.oid(),
//         })
//         .collect();
//     let table_schema = TableSchema {
//         relation_id: table_schema.relation_id,
//         schema: table_schema.table.schema.clone(),
//         table: table_schema.table.name.clone(),
//         attributes,
//     };
//     writer
//         .append_ser(table_schema)
//         .expect("failed to write table schema");
//     let encoded = writer.into_inner().expect("failed to encode");
//     println!("ENCODED: LEN: {}, {encoded:?}", encoded.len());
//     let reader = Reader::new(&encoded[..]).expect("failed to create reader");
//     for value in reader {
//         println!("{:?}", value.expect("failed to read value"));
//     }
// }

fn _a() {
    //
    #[derive(Debug, Serialize)]
    struct Test {
        a: i64,
        b: String,
    }

    let raw_schema = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "a", "type": "long", "default": 42},
                {"name": "b", "type": "string"}
            ]
        }
    "#;

    // if the schema is not valid, this function will return an error
    let schema = Schema::parse_str(raw_schema).unwrap();

    // a writer needs a schema and something to write to
    let mut writer = Writer::new(&schema, Vec::new());

    // the structure models our Record schema
    let test = Test {
        a: 27,
        b: "foo".to_owned(),
    };

    // schema validation happens here
    writer.append_ser(test).unwrap();

    // this is how to get back the resulting avro bytecode
    // this performs a flush operation to make sure data is written, so it can fail
    // you can also call `writer.flush()` yourself without consuming the writer
    let _encoded = writer.into_inner();
}
