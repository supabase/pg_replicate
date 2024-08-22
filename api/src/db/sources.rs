use sqlx::PgPool;
use std::fmt::{Debug, Formatter};

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SourceConfig {
    Postgres {
        /// Host on which Postgres is running
        host: String,

        /// Port on which Postgres is running
        port: u16,

        /// Postgres database name
        name: String,

        /// Postgres database user name
        username: String,

        //TODO: encrypt before storing in db
        /// Postgres database user password
        password: Option<String>,

        /// Postgres slot name
        slot_name: String,

        /// Postgres publication name
        publication: String,
    },
}

impl Debug for SourceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceConfig::Postgres {
                host,
                port,
                name,
                username,
                password: _,
                slot_name,
                publication,
            } => f
                .debug_struct("Postgres")
                .field("host", host)
                .field("port", port)
                .field("name", name)
                .field("username", username)
                .field("password", &"REDACTED")
                .field("slot_name", slot_name)
                .field("publication", publication)
                .finish(),
        }
    }
}

pub struct Source {
    pub id: i64,
    pub tenant_id: i64,
    pub config: serde_json::Value,
}

pub async fn create_source(
    pool: &PgPool,
    tenant_id: i64,
    config: &SourceConfig,
) -> Result<i64, sqlx::Error> {
    let config = serde_json::to_value(config).expect("failed to serialize config");
    let record = sqlx::query!(
        r#"
        insert into sources (tenant_id, config)
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

pub async fn read_source(
    pool: &PgPool,
    tenant_id: i64,
    source_id: i64,
) -> Result<Option<Source>, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        select id, tenant_id, config
        from sources
        where tenant_id = $1 and id = $2
        "#,
        tenant_id,
        source_id,
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| Source {
        id: r.id,
        tenant_id: r.tenant_id,
        config: r.config,
    }))
}

pub async fn update_source(
    pool: &PgPool,
    tenant_id: i64,
    source_id: i64,
    config: &SourceConfig,
) -> Result<Option<i64>, sqlx::Error> {
    let config = serde_json::to_value(config).expect("failed to serialize config");
    let record = sqlx::query!(
        r#"
        update sources
        set config = $1
        where tenant_id = $2 and id = $3
        returning id
        "#,
        config,
        tenant_id,
        source_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| r.id))
}
