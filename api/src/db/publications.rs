use sqlx::PgPool;

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct PublicationConfig {
    pub table_names: Vec<String>,
}

pub struct Publication {
    pub id: i64,
    pub tenant_id: i64,
    pub source_id: i64,
    pub config: serde_json::Value,
}

pub async fn create_publication(
    pool: &PgPool,
    tenant_id: i64,
    source_id: i64,
    config: &PublicationConfig,
) -> Result<i64, sqlx::Error> {
    let config = serde_json::to_value(config).expect("failed to serialize config");
    let record = sqlx::query!(
        r#"
        insert into publications (tenant_id, source_id, config)
        values ($1, $2, $3)
        returning id
        "#,
        tenant_id,
        source_id,
        config
    )
    .fetch_one(pool)
    .await?;

    Ok(record.id)
}

pub async fn read_publication(
    pool: &PgPool,
    tenant_id: i64,
    publication_id: i64,
) -> Result<Option<Publication>, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        select id, tenant_id, source_id, config
        from publications
        where tenant_id = $1 and id = $2
        "#,
        tenant_id,
        publication_id,
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| Publication {
        id: r.id,
        tenant_id: r.tenant_id,
        source_id: r.source_id,
        config: r.config,
    }))
}

pub async fn update_publication(
    pool: &PgPool,
    tenant_id: i64,
    publication_id: i64,
    source_id: i64,
    config: &PublicationConfig,
) -> Result<Option<i64>, sqlx::Error> {
    let config = serde_json::to_value(config).expect("failed to serialize config");
    let record = sqlx::query!(
        r#"
        update publications
        set source_id = $1, config = $2
        where tenant_id = $3 and id = $4
        returning id
        "#,
        source_id,
        config,
        tenant_id,
        publication_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn delete_publication(
    pool: &PgPool,
    tenant_id: i64,
    publication_id: i64,
) -> Result<Option<i64>, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        delete from publications
        where tenant_id = $1 and id = $2
        returning id
        "#,
        tenant_id,
        publication_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn read_all_publications(
    pool: &PgPool,
    tenant_id: i64,
) -> Result<Vec<Publication>, sqlx::Error> {
    let mut record = sqlx::query!(
        r#"
        select id, tenant_id, source_id, config
        from publications
        where tenant_id = $1
        "#,
        tenant_id,
    )
    .fetch_all(pool)
    .await?;

    Ok(record
        .drain(..)
        .map(|r| Publication {
            id: r.id,
            tenant_id: r.tenant_id,
            source_id: r.source_id,
            config: r.config,
        })
        .collect())
}
