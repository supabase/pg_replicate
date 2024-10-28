use sqlx::{PgPool, Postgres, Transaction};

#[derive(Clone, Debug, PartialEq, PartialOrd, sqlx::Type)]
#[sqlx(type_name = "app.replicator_status", rename_all = "lowercase")]
pub enum ReplicatorStatus {
    Stopped,
    Starting,
    Started,
    Stopping,
}

pub struct Replicator {
    pub id: i64,
    pub tenant_id: String,
    pub image_id: i64,
    pub status: ReplicatorStatus,
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
        insert into app.replicators (tenant_id, image_id, status)
        values ($1, $2, $3::app.replicator_status)
        returning id
        "#,
        tenant_id,
        image_id,
        ReplicatorStatus::Stopped as ReplicatorStatus
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
        select r.id, r.tenant_id, r.image_id, r.status as "status: ReplicatorStatus"
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
        status: r.status,
    }))
}
