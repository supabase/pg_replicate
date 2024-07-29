use sqlx::{PgPool, Postgres, Transaction};

pub async fn enqueue_task(
    pool: &PgPool,
    task_name: &str,
    task_data: serde_json::Value,
) -> Result<i64, anyhow::Error> {
    let row: (i64,) = sqlx::query_as(
        r#"
        insert into queue.task_queue (name, data)
        values($1, $2) returning id
        "#,
    )
    .bind(task_name)
    .bind(task_data)
    .fetch_one(pool)
    .await?;

    Ok(row.0)
}

type PgTransaction = Transaction<'static, Postgres>;

#[derive(Debug, sqlx::FromRow)]
pub struct Task {
    pub id: i64,
    pub name: String,
    pub data: serde_json::Value,
}

pub async fn dequeue_task(pool: &PgPool) -> Result<Option<(PgTransaction, Task)>, anyhow::Error> {
    let mut txn = pool.begin().await?;

    let res = sqlx::query_as::<_, Task>(
        r#"
        select id, name, data
        from queue.task_queue
        order by id
        limit 1
        for update
        skip locked
        "#,
    )
    .fetch_optional(&mut *txn)
    .await?
    .map(|task| (txn, task));

    Ok(res)
}

pub async fn delete_task(mut txn: PgTransaction, id: i64) -> Result<(), anyhow::Error> {
    sqlx::query(
        r#"
        delete from queue.task_queue
        where id = $1
        "#,
    )
    .bind(id)
    .execute(&mut *txn)
    .await?;

    txn.commit().await?;

    Ok(())
}
