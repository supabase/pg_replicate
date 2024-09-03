use sqlx::{PgPool, Postgres, Transaction};

pub struct Image {
    pub id: i64,
    pub image_name: String,
    pub is_default: bool,
}

pub async fn create_image(
    pool: &PgPool,
    image_name: &str,
    is_default: bool,
) -> Result<i64, sqlx::Error> {
    let mut txn = pool.begin().await?;
    let res = create_image_txn(&mut txn, image_name, is_default).await;
    txn.commit().await?;
    res
}

pub async fn create_image_txn(
    txn: &mut Transaction<'_, Postgres>,
    image_name: &str,
    is_default: bool,
) -> Result<i64, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        insert into images (image_name, is_default)
        values ($1, $2)
        returning id
        "#,
        image_name,
        is_default
    )
    .fetch_one(&mut **txn)
    .await?;

    Ok(record.id)
}

pub async fn read_default_image(pool: &PgPool) -> Result<Option<Image>, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        select id, image_name, is_default
        from images
        where is_default = true
        "#,
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| Image {
        id: r.id,
        image_name: r.image_name,
        is_default: r.is_default,
    }))
}

pub async fn read_image_by_replicator_id(
    pool: &PgPool,
    replicator_id: i64,
) -> Result<Option<Image>, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        select i.id, i.image_name, i.is_default
        from images i
        join replicators r on i.id = r.image_id
        where r.id = $1
        "#,
        replicator_id,
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| Image {
        id: r.id,
        image_name: r.image_name,
        is_default: r.is_default,
    }))
}
