use sqlx::{PgPool, Postgres, Transaction};

pub async fn enqueue_task(
    pool: &PgPool,
    task_name: &str,
    task_data: serde_json::Value,
) -> Result<i64, sqlx::Error> {
    let task = sqlx::query!(
        r#"
        insert into app.task_queue (name, data)
        values($1, $2) returning id
        "#,
        task_name,
        task_data
    )
    .fetch_one(pool)
    .await?;

    Ok(task.id)
}

type PgTransaction = Transaction<'static, Postgres>;

#[derive(Debug)]
pub struct Task {
    pub id: i64,
    pub name: String,
    pub data: serde_json::Value,
}

pub async fn dequeue_task(pool: &PgPool) -> Result<Option<(PgTransaction, Task)>, anyhow::Error> {
    let mut txn = pool.begin().await?;

    let res = sqlx::query!(
        r#"
        select id, name, data
        from app.task_queue
        order by id
        limit 1
        for update
        skip locked
        "#,
    )
    .fetch_optional(&mut *txn)
    .await?
    .map(|task| {
        (
            txn,
            Task {
                id: task.id,
                name: task.name,
                data: task.data,
            },
        )
    });

    Ok(res)
}

pub async fn delete_task(mut txn: PgTransaction, id: i64) -> Result<(), anyhow::Error> {
    sqlx::query!(
        r#"
        delete from app.task_queue
        where id = $1
        "#,
        id
    )
    .execute(&mut *txn)
    .await?;

    txn.commit().await?;

    Ok(())
}
