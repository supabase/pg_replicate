use core::str;

use bytes::{BufMut, BytesMut};
use chrono::{DateTime, Utc};
use thiserror::Error;
use tokio_postgres::{
    types::{to_sql_checked, FromSql, IsNull, Kind, ToSql},
    Client,
};

#[derive(Debug)]
pub enum TaskStatus {
    Pending,
    InProgress,
    Done,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Pending => "pending",
            TaskStatus::InProgress => "in_progress",
            TaskStatus::Done => "done",
        }
    }
}

#[derive(Error, Debug)]
pub enum TaskStatusConversionError {
    #[error("invalid task status: {0}")]
    InvalidTaskStatus(String),
}

impl TryFrom<&str> for TaskStatus {
    type Error = TaskStatusConversionError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "pending" => Ok(TaskStatus::Pending),
            "in_progress" => Ok(TaskStatus::InProgress),
            "done" => Ok(TaskStatus::Done),
            value => Err(TaskStatusConversionError::InvalidTaskStatus(
                value.to_string(),
            )),
        }
    }
}

impl ToSql for TaskStatus {
    fn to_sql(
        &self,
        _ty: &tokio_postgres::types::Type,
        out: &mut BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        out.put_slice(self.as_str().as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool
    where
        Self: Sized,
    {
        ty.name() == "task_status" && matches!(ty.kind(), Kind::Enum(_)) && ty.schema() == "queue"
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for TaskStatus {
    fn from_sql(
        _ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let task_status_str = str::from_utf8(raw)?;
        Ok(task_status_str.try_into()?)
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        ty.name() == "task_status" && matches!(ty.kind(), Kind::Enum(_)) && ty.schema() == "queue"
    }
}

pub async fn enqueue(
    client: &Client,
    task_name: &str,
    task_data: serde_json::Value,
    task_status: TaskStatus,
) -> Result<i64, tokio_postgres::Error> {
    let row = client
        .query_one(
            r#"
            insert into queue.task_queue (name, data, status)
            values($1, $2, $3) returning id"#,
            &[&task_name, &task_data, &task_status],
        )
        .await?;
    Ok(row.get(0))
}

#[derive(Debug)]
pub struct Task {
    pub id: i64,
    pub name: String,
    pub data: serde_json::Value,
    pub status: TaskStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub async fn dequeue(
    client: &mut Client,
    max_tasks: i64,
) -> Result<Vec<Task>, tokio_postgres::Error> {
    let txn = client.transaction().await?;

    let rows = txn
        .query(
            r#"
        select id, name, data, status, created_at, updated_at
        from queue.task_queue
        where status = 'pending'
        order by id
        limit $1
        for update
        skip locked"#,
            &[&max_tasks],
        )
        .await?;

    let tasks: Vec<Task> = rows
        .iter()
        .map(|row| Task {
            id: row.get(0),
            name: row.get(1),
            data: row.get(2),
            status: row.get(3),
            created_at: row.get(4),
            updated_at: row.get(5),
        })
        .collect();

    let task_ids: Vec<i64> = tasks.iter().map(|task| task.id).collect();

    txn.execute(
        r#"
        update queue.task_queue
        set status = 'in_progress', updated_at = now()
        where id in (select unnest($1::bigint[]))"#,
        &[&task_ids],
    )
    .await?;

    txn.commit().await?;

    Ok(tasks)
}
