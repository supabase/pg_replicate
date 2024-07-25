use bytes::{BufMut, BytesMut};
use tokio_postgres::{
    types::{to_sql_checked, IsNull, ToSql},
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
        ty.name() == "task_status"
    }

    to_sql_checked!();
}

pub async fn enqueue(
    client: &Client,
    task_name: &str,
    task_data: serde_json::Value,
    task_status: TaskStatus,
) -> Result<i32, tokio_postgres::Error> {
    let row = client
        .query_one(
            "insert into queue.task_queue(name, data, status) values($1, $2, $3) returning id",
            &[&task_name, &task_data, &task_status],
        )
        .await?;
    Ok(row.get(0))
}
