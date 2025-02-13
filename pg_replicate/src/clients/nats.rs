use std::{collections::HashMap, time::Duration};

use anyhow::anyhow;
use async_nats::{header::NATS_MESSAGE_ID, jetstream, ConnectError, ConnectOptions, HeaderMap};
use async_trait::async_trait;
use bytes::{Buf, BufMut, BytesMut};
use serde_json::json;
use sha256::digest;
use tokio_postgres::types::PgLsn;
use tracing::{error, info, warn};

use crate::{
    conversions::{table_row::TableRow, Cell},
    table::TableSchema,
};

#[async_trait]
pub trait MessageMapper {
    fn map(
        &self,
        table_id: u32,
        row: TableRow,
        schema: &TableSchema,
    ) -> Result<serde_json::Value, serde_json::Error>;
}

#[async_trait]
pub trait SubjectMapper {
    fn map(
        &self,
        row: serde_json::Value
    ) -> Result<String, anyhow::Error>;
}

pub struct NatsClient<M: MessageMapper + Send + Sync, S: SubjectMapper + Send + Sync> {
    conn: jetstream::Context,
    message_mapper: M,
    subject_mapper: S
}

impl<M: MessageMapper + Send + Sync, S: SubjectMapper + Send + Sync> NatsClient<M, S> {
    pub async fn new(address: String, message_mapper: M, subject_mapper: S) -> Result<NatsClient<M, S>, ConnectError> {
        let client = async_nats::connect_with_options(
            address,
            ConnectOptions::new()
                .no_echo()
                .ping_interval(Duration::from_secs(5))
                .connection_timeout(Duration::from_secs(5))
                .event_callback(|e| async move {
                    match e {
                        async_nats::Event::Connected => info!("{e}"),
                        async_nats::Event::Disconnected => error!("{e}"),
                        async_nats::Event::ServerError(_) => error!("{e}"),
                        async_nats::Event::ClientError(_) => error!("{e}"),
                        _ => warn!("{e}"),
                    }
                }),
        )
        .await?;
        let jetstream = async_nats::jetstream::new(client);

        return Ok(Self {
            conn: jetstream,
            message_mapper,
            subject_mapper
        });
    }

    pub async fn bucket_exists(&self) -> bool {
        let response = self.conn.get_key_value("postgres_cdc_lsn").await;
        return response.is_ok();
    }

    pub async fn create_bucket(&self) -> Result<(), async_nats::Error> {
        let _ = self
            .conn
            .create_key_value(jetstream::kv::Config {
                bucket: "postgres_cdc_lsn".into(),
                ..Default::default()
            })
            .await?;
        return Ok(());
    }

    pub async fn insert_last_lsn_row(&self) -> Result<(), async_nats::Error> {
        let store = self.conn.get_key_value("postgres_cdc_lsn").await?;
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u64(0);
        store.put("last_lsn", buf.freeze()).await?;
        Ok(())
    }

    pub async fn get_last_lsn(&self) -> Result<PgLsn, async_nats::Error> {
        let store = self.conn.get_key_value("postgres_cdc_lsn").await?;
        let response = store.get("last_lsn").await?;
        if response.is_none() {
            return Err(anyhow!("no data in the 'last_lsn' key/value").into());
        }
        let mut buf = BytesMut::with_capacity(8);
        buf.put_slice(&response.unwrap());
        let mut buf = buf.freeze();
        let lsn = buf.get_u64();
        Ok(lsn.into())
    }

    pub async fn set_last_lsn(&self, lsn: PgLsn) -> Result<(), async_nats::Error> {
        let store = self.conn.get_key_value("postgres_cdc_lsn").await?;
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u64(lsn.into());
        store.put("last_lsn", buf.freeze()).await?;
        Ok(())
    }

    pub async fn publish(
        &self,
        table_id: u32,
        row: TableRow,
        schema: &TableSchema,
    ) -> Result<(), async_nats::Error> {
        let payload = self.message_mapper.map(table_id, row, schema)?;
        let serialized: String = payload.to_string();

        let mut headers: HeaderMap = HeaderMap::new();
        let sha256 = digest(serialized.clone());

        headers.insert(NATS_MESSAGE_ID, sha256.as_str());

        let serialized: String = payload.to_string();

        let topic = self.subject_mapper.map(payload)?;

        self.conn
            .publish_with_headers(topic, headers, serialized.into())
            .await?;

        Ok(())
    }
}
