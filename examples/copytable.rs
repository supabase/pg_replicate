use std::error::Error;

use pg_replicate::CopyTableClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = CopyTableClient::new(
        "localhost".to_string(),
        8080,
        "testdb".to_string(),
        "raminder.singh".to_string(),
    );

    client.copy_table("public", "table1").await?;

    Ok(())
}
