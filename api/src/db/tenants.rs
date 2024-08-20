use sqlx::PgPool;

pub struct Tenant {
    pub id: i64,
    pub name: String,
}

pub async fn create_tenant(pool: &PgPool, tenant_name: &str) -> Result<i64, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        insert into tenants (name)
        values ($1)
        returning id
        "#,
        tenant_name
    )
    .fetch_one(pool)
    .await?;

    Ok(record.id)
}

pub async fn read_tenant(pool: &PgPool, tenant_id: i64) -> Result<Option<Tenant>, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        select id, name
        from tenants
        where id = $1
        "#,
        tenant_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| Tenant {
        id: r.id,
        name: r.name,
    }))
}

pub async fn update_tenant(
    pool: &PgPool,
    tenant_id: i64,
    tenant_name: &str,
) -> Result<Option<i64>, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        update tenants
        set name = $1
        where id = $2
        returning id
        "#,
        tenant_name,
        tenant_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn delete_tenant(pool: &PgPool, tenant_id: i64) -> Result<Option<i64>, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        delete from tenants
        where id = $1
        returning id
        "#,
        tenant_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| r.id))
}
