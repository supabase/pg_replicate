use core::str;

use tokio_postgres::{Client, Transaction};

pub async fn enqueue(
    client: &Client,
    task_name: &str,
    task_data: serde_json::Value,
) -> Result<i64, tokio_postgres::Error> {
    let row = client
        .query_one(
            r#"
            insert into queue.task_queue (name, data)
            values($1, $2) returning id"#,
            &[&task_name, &task_data],
        )
        .await?;
    Ok(row.get(0))
}

#[derive(Debug)]
pub struct Task {
    pub id: i64,
    pub name: String,
    pub data: serde_json::Value,
}

pub async fn dequeue(
    client: &mut Client,
) -> Result<(Transaction, Option<Task>), tokio_postgres::Error> {
    let txn = client.transaction().await?;

    let row = txn
        .query_opt(
            r#"
        select id, name, data
        from queue.task_queue
        order by id
        limit 1
        for update
        skip locked"#,
            &[],
        )
        .await?;

    let task = row.map(|row| Task {
        id: row.get(0),
        name: row.get(1),
        data: row.get(2),
    });

    Ok((txn, task))
}

pub async fn delete_task(txn: Transaction<'_>, id: i64) -> Result<(), tokio_postgres::Error> {
    txn.execute(
        r#"
        delete from queue.task_queue
        where id = $1
    "#,
        &[&id],
    )
    .await?;

    txn.commit().await?;

    Ok(())
}
