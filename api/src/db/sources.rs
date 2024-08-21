use sqlx::PgPool;

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

pub struct Source {
    pub id: i64,
    pub tenant_id: i64,
    pub config: SourceConfig,
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
