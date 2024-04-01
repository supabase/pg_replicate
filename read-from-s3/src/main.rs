use std::{collections::HashMap, error::Error, io::Write};

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{config::Credentials, types::ObjectIdentifier, Client};
use chrono::{DateTime, Utc};
use pg_replicate::EventType;
use serde::{Deserialize, Serialize};
use serde_cbor::Value;

#[derive(Serialize, Deserialize, Debug)]
struct Event {
    event_type: EventType,
    timestamp: DateTime<Utc>,
    relation_id: Option<u32>,
    last_lsn: u64,
    data: Value,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let credentials = Credentials::new("admin", "password", None, None, "example");
    let s3_config = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url("http://localhost:9000")
        .credentials_provider(credentials)
        .region(Region::new("eu-central-1"))
        .force_path_style(true) // apply bucketname as path param instead of pre-domain
        .build();
    let client = aws_sdk_s3::Client::from_conf(s3_config);
    let bucket_name = "test-rust-s3";
    read_table_copies(&client, bucket_name).await?;

    Ok(())
}

pub async fn read_table_copies(client: &Client, bucket_name: &str) -> Result<(), anyhow::Error> {
    let s3_prefix = "table_copies/";
    let objects = list_objects(client, bucket_name, s3_prefix).await?;
    let mut table_copy_files: HashMap<&str, (bool, Vec<&str>)> = HashMap::new();
    for obj in &objects {
        let tokens: Vec<&str> = obj.key.split('/').collect();
        if tokens.len() == 3 {
            let (done, files) = table_copy_files.entry(tokens[1]).or_default();
            if tokens[2] == "done" {
                *done = true;
            }
            files.push(tokens[2]);
        } else {
            //todo: error?
        }
    }

    for (key, (done, files)) in table_copy_files {
        print!("Table {key} ");
        if done {
            print!("is ");
        } else {
            print!("is not ");
        }
        println!("fully copied with the following files:");
        let mut files: Vec<u32> = files
            .iter()
            .filter(|&&file| file != "done")
            .map(|file| {
                let file_int: u32 = file.parse().expect("file is not an integer");
                file_int
            })
            .collect();

        files.sort();

        for file in files {
            let file_name = format!("table_copies/{key}/{file}");
            print_events_in_file(client, bucket_name, &file_name).await?;
        }
    }

    Ok(())
}

pub async fn print_events_in_file(
    client: &Client,
    bucket_name: &str,
    file_name: &str,
) -> Result<(), anyhow::Error> {
    let mut file_contents = client
        .get_object()
        .bucket(bucket_name)
        .key(file_name)
        .send()
        .await?;

    let mut v = vec![];
    while let Some(bytes) = file_contents.body.try_next().await? {
        v.write_all(&bytes)?;
    }
    let mut start = 0;
    loop {
        let size: [u8; 8] = (&v[start..start + 8]).try_into()?;
        let size = usize::from_be_bytes(size);
        let new_start = start + 8 + size;
        let event_data = &v[start + 8..new_start];
        let event: Event = serde_cbor::from_reader(event_data)?;
        println!("Event: {event:#?}");
        start = new_start;
        if v.len() <= new_start {
            break;
        }
    }

    Ok(())
}

pub async fn list_objects(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<ObjectIdentifier>, anyhow::Error> {
    let mut response = client
        .list_objects_v2()
        .bucket(bucket.to_owned())
        .prefix(prefix)
        .max_keys(100)
        .into_paginator()
        .send();

    let mut objects = vec![];
    while let Some(result) = response.next().await {
        match result {
            Ok(output) => {
                for object in output.contents() {
                    let obj_id = ObjectIdentifier::builder()
                        .set_key(Some(object.key().expect("missing key").to_string()))
                        .build()?;
                    objects.push(obj_id);
                }
            }
            Err(err) => {
                Err(err)?;
            }
        }
    }

    Ok(objects)
}
