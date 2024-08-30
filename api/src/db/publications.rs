use sqlx::PgPool;

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct PublicationConfig {
    pub table_names: Vec<String>,
}

pub struct Publication {
    pub id: i64,
    pub tenant_id: i64,
    pub source_id: i64,
    pub name: String,
    pub config: serde_json::Value,
}

pub async fn create_publication(
    pool: &PgPool,
    tenant_id: i64,
    source_id: i64,
    name: String,
    config: &PublicationConfig,
) -> Result<i64, sqlx::Error> {
    let config = serde_json::to_value(config).expect("failed to serialize config");
    let record = sqlx::query!(
        r#"
        insert into publications (tenant_id, source_id, name, config)
        values ($1, $2, $3, $4)
        returning id
        "#,
        tenant_id,
        source_id,
        name,
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
        select id, tenant_id, source_id, name, config
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
        name: r.name,
        config: r.config,
    }))
}

pub async fn update_publication(
    pool: &PgPool,
    tenant_id: i64,
    publication_id: i64,
    source_id: i64,
    name: String,
    config: &PublicationConfig,
) -> Result<Option<i64>, sqlx::Error> {
    let config = serde_json::to_value(config).expect("failed to serialize config");
    let record = sqlx::query!(
        r#"
        update publications
        set source_id = $1, name = $2, config = $3
        where tenant_id = $4 and id = $5
        returning id
        "#,
        source_id,
        name,
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
        select id, tenant_id, source_id, name, config
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
            name: r.name,
            config: r.config,
        })
        .collect())
}

pub async fn publication_exists(
    pool: &PgPool,
    tenant_id: i64,
    publication_id: i64,
) -> Result<bool, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        select exists (select id
        from publications
        where tenant_id = $1 and id = $2)
        "#,
        tenant_id,
        publication_id,
    )
    .fetch_one(pool)
    .await?;

    Ok(record
        .exists
        .expect("select exists always returns a non-null value"))
}
