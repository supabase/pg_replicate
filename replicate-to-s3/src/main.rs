use std::{error::Error, io::Write};

use anyhow::anyhow;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{config::Credentials, Client};
use chrono::{DateTime, Utc};
use clap::Parser;
use pg_replicate::{EventType, ReplicationClient, ResumptionData};
use serde::{Deserialize, Serialize};
use serde_cbor::Value;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long)]
    s3_username: String,
    #[arg(long)]
    s3_password: String,
    #[arg(long)]
    s3_base_url: String,
    #[arg(long)]
    s3_region: String,
    #[arg(long)]
    s3_bucket_name: String,
    #[arg(long)]
    db_host: String,
    #[arg(long)]
    db_port: u16,
    #[arg(long)]
    db_name: String,
    #[arg(long)]
    db_username: String,
    #[arg(long)]
    db_password: Option<String>,
    #[arg(long)]
    db_slot_name: String,
}

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
    let args = Args::parse();

    let credentials = Credentials::new(
        &args.s3_username,
        &args.s3_password,
        None,
        None,
        "command line",
    );

    let s3_config = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(&args.s3_base_url)
        .credentials_provider(credentials)
        .region(Region::new(args.s3_region))
        .force_path_style(true)
        .build();

    let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

    let resumption_data = get_relatime_resumption_data(&s3_client, &args.s3_bucket_name).await?;

    let _db_client = ReplicationClient::new(
        args.db_host,
        args.db_port,
        args.db_name,
        args.db_username,
        args.db_slot_name,
        resumption_data,
    );

    Ok(())
}

const REALTIME_CHANGES_PREFIX: &str = "realtime_changes/";

async fn get_relatime_resumption_data(
    client: &Client,
    bucket_name: &str,
) -> Result<Option<ResumptionData>, anyhow::Error> {
    let Some(last_file_name) =
        largest_realtime_file_number(client, bucket_name, REALTIME_CHANGES_PREFIX).await?
    else {
        return Ok(None);
    };
    let object_prefix = format!("{REALTIME_CHANGES_PREFIX}{last_file_name}");

    let mut last_file = client
        .get_object()
        .bucket(bucket_name)
        .key(object_prefix)
        .send()
        .await?;

    let mut v = vec![];
    while let Some(bytes) = last_file.body.try_next().await? {
        v.write_all(&bytes)?;
    }

    let mut start = 0;
    let mut v = &v[..];
    loop {
        let size: [u8; 8] = (&v[start..start + 8]).try_into()?;
        let size = usize::from_be_bytes(size);
        let new_start = start + 8 + size;
        if v.len() <= new_start {
            v = &v[start + 8..];
            break;
        }
        start = new_start;
    }

    let event: Event = serde_cbor::from_reader(v)?;

    Ok(Some(ResumptionData {
        resume_lsn: event.last_lsn.into(),
        last_event_type: event.event_type,
        last_file_name,
        skipping_events: event.event_type != EventType::Commit,
    }))
}

/// Returns the largest numbered file in with the realtime_changes/
/// prefix.
///
/// This function will get slower over time as the S3 client needs
/// to make multiple calls to get the largest key because in one
/// call S3 API returns maximum 1000 keys.
pub async fn largest_realtime_file_number(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<Option<u32>, anyhow::Error> {
    let mut response = client
        .list_objects_v2()
        .bucket(bucket.to_owned())
        .prefix(prefix)
        .into_paginator()
        .send();

    let mut largest = None;

    while let Some(result) = response.next().await {
        for object in result?.contents() {
            let key = object
                .key()
                .ok_or(anyhow!("missing key"))?
                .strip_prefix(REALTIME_CHANGES_PREFIX)
                .ok_or(anyhow!("wrong prefix"))?;
            let key: u32 = key.parse()?;
            if let Some(last_largest) = largest {
                if key > last_largest {
                    largest = Some(key);
                }
            } else {
                largest = Some(key);
            }
        }
    }

    Ok(largest)
}
