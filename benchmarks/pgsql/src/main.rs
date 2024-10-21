use dotenv::dotenv;
use postgres::{Client, NoTls};
use std::env;

mod error;
mod models;
mod schema;

#[tokio::main]
async fn main() -> error::Result<()> {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let mut client = match Client::connect(&database_url, NoTls) {
        Ok(client) => client,
        Err(e) => return Err(error::BenchmarkError::DatabaseError(e.to_string())),
    };

    match schema::create_tables(&mut client).await {
        Ok(_) => println!("Tables created successfully"),
        Err(e) => return Err(e),
    }

    // Perform a simple test query
    let result = match client.query("SELECT 1 AS result", &[]) {
        Ok(rows) => rows[0].get("result"),
        Err(e) => return Err(error::BenchmarkError::DatabaseError(e.to_string())),
    };
    println!("Test query result: {}", result);

    Ok(())
}
