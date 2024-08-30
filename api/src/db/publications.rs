use std::borrow::Cow;

use sqlx::{postgres::PgConnectOptions, Connection, Executor, PgConnection};

use super::tables::Table;

pub fn quote_identifier(identifier: &str) -> Cow<'_, str> {
    if identifier.find('"').is_some() {
        Cow::Owned(quote_identifier_alloc(identifier))
    } else {
        Cow::Borrowed(identifier)
    }
}

fn quote_identifier_alloc(identifier: &str) -> String {
    let mut quoted_identifier = String::with_capacity(identifier.len());
    for char in identifier.chars() {
        if char == '"' {
            quoted_identifier.push('"');
        }
        quoted_identifier.push(char);
    }
    quoted_identifier
}

pub async fn create_publication_on_source(
    publication_name: &str,
    tables: &[Table],
    options: &PgConnectOptions,
) -> Result<(), sqlx::Error> {
    let mut query = String::new();
    let quoted_publication_name = quote_identifier(publication_name);
    query.push_str("create publication ");
    query.push_str(&quoted_publication_name);
    query.push_str(" table only ");

    for (i, table) in tables.iter().enumerate() {
        let quoted_schema = quote_identifier(&table.schema);
        let quoted_name = quote_identifier(&table.name);
        query.push_str(&quoted_schema);
        query.push('.');
        query.push_str(&quoted_name);

        if i < tables.len() - 1 {
            query.push(',')
        }
    }

    let mut connection = PgConnection::connect_with(options).await?;
    connection.execute(query.as_str()).await?;

    Ok(())
}
