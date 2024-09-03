use sqlx::PgPool;
use std::fmt::{Debug, Formatter};

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SinkConfig {
    BigQuery {
        /// BigQuery project id
        project_id: String,

        /// BigQuery dataset id
        dataset_id: String,

        //TODO: encrypt before storing in db
        /// BigQuery service account key
        service_account_key: String,
    },
}

impl Debug for SinkConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key: _,
            } => f
                .debug_struct("BigQuery")
                .field("project_id", project_id)
                .field("dataset_id", dataset_id)
                .field("service_account_key", &"REDACTED")
                .finish(),
        }
    }
}

pub struct Sink {
    pub id: i64,
    pub tenant_id: i64,
    pub config: serde_json::Value,
}

pub async fn create_sink(
    pool: &PgPool,
    tenant_id: i64,
    config: &SinkConfig,
) -> Result<i64, sqlx::Error> {
    let config = serde_json::to_value(config).expect("failed to serialize config");
    let record = sqlx::query!(
        r#"
        insert into sinks (tenant_id, config)
        values ($1, $2)
        returning id
        "#,
        tenant_id,
        config
    )
    .fetch_one(pool)
    .await?;

    Ok(record.id)
}

pub async fn read_sink(
    pool: &PgPool,
    tenant_id: i64,
    sink_id: i64,
) -> Result<Option<Sink>, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        select id, tenant_id, config
        from sinks
        where tenant_id = $1 and id = $2
        "#,
        tenant_id,
        sink_id,
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| Sink {
        id: r.id,
        tenant_id: r.tenant_id,
        config: r.config,
    }))
}

pub async fn update_sink(
    pool: &PgPool,
    tenant_id: i64,
    sink_id: i64,
    config: &SinkConfig,
) -> Result<Option<i64>, sqlx::Error> {
    let config = serde_json::to_value(config).expect("failed to serialize config");
    let record = sqlx::query!(
        r#"
        update sinks
        set config = $1
        where tenant_id = $2 and id = $3
        returning id
        "#,
        config,
        tenant_id,
        sink_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn delete_sink(
    pool: &PgPool,
    tenant_id: i64,
    sink_id: i64,
) -> Result<Option<i64>, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        delete from sinks
        where tenant_id = $1 and id = $2
        returning id
        "#,
        tenant_id,
        sink_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn read_all_sinks(pool: &PgPool, tenant_id: i64) -> Result<Vec<Sink>, sqlx::Error> {
    let mut record = sqlx::query!(
        r#"
        select id, tenant_id, config
        from sinks
        where tenant_id = $1
        "#,
        tenant_id,
    )
    .fetch_all(pool)
    .await?;

    Ok(record
        .drain(..)
        .map(|r| Sink {
            id: r.id,
            tenant_id: r.tenant_id,
            config: r.config,
        })
        .collect())
}

pub async fn sink_exists(pool: &PgPool, tenant_id: i64, sink_id: i64) -> Result<bool, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        select exists (select id
        from sinks
        where tenant_id = $1 and id = $2) as "exists!"
        "#,
        tenant_id,
        sink_id,
    )
    .fetch_one(pool)
    .await?;

    Ok(record.exists)
}
