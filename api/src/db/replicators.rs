use sqlx::{PgPool, Postgres, Transaction};

pub struct Replicator {
    pub id: i64,
    pub tenant_id: String,
    pub image_id: i64,
}

pub async fn create_replicator(
    pool: &PgPool,
    tenant_id: &str,
    image_id: i64,
) -> Result<i64, sqlx::Error> {
    let mut txn = pool.begin().await?;
    let res = create_replicator_txn(&mut txn, tenant_id, image_id).await;
    txn.commit().await?;
    res
}

pub async fn create_replicator_txn(
    txn: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    image_id: i64,
) -> Result<i64, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        insert into app.replicators (tenant_id, image_id)
        values ($1, $2)
        returning id
        "#,
        tenant_id,
        image_id
    )
    .fetch_one(&mut **txn)
    .await?;

    Ok(record.id)
}

pub async fn read_replicator_by_pipeline_id(
    pool: &PgPool,
    tenant_id: &str,
    pipeline_id: i64,
) -> Result<Option<Replicator>, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        select r.id, r.tenant_id, r.image_id
        from app.replicators r
        join app.pipelines p on r.id = p.replicator_id
        where r.tenant_id = $1 and p.tenant_id = $1 and p.id = $2
        "#,
        tenant_id,
        pipeline_id,
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| Replicator {
        id: r.id,
        tenant_id: r.tenant_id,
        image_id: r.image_id,
    }))
}
