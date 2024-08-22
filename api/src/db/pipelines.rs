use sqlx::PgPool;

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct PipelineConfig {
    pub config: BatchConfig,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct BatchConfig {
    /// maximum batch size in number of events
    pub max_size: usize,

    /// maximum duration, in seconds, to wait for a batch to fill
    pub max_fill_secs: u64,
}

pub struct Pipeline {
    pub id: i64,
    pub tenant_id: i64,
    pub source_id: i64,
    pub sink_id: i64,
    pub config: serde_json::Value,
}

pub async fn create_pipeline(
    pool: &PgPool,
    tenant_id: i64,
    source_id: i64,
    sink_id: i64,
    config: &PipelineConfig,
) -> Result<i64, sqlx::Error> {
    let config = serde_json::to_value(config).expect("failed to serialize config");
    let record = sqlx::query!(
        r#"
        insert into pipelines (tenant_id, source_id, sink_id, config)
        values ($1, $2, $3, $4)
        returning id
        "#,
        tenant_id,
        source_id,
        sink_id,
        config
    )
    .fetch_one(pool)
    .await?;

    Ok(record.id)
}
