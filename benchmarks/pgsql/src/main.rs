use dotenv::dotenv;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use std::env;
use tokio_postgres::{Client, NoTls};

mod error;
mod models;
mod schema;

#[tokio::main]
async fn main() -> error::Result<()> {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let tls_connector = match TlsConnector::builder().build() {
        Ok(connector) => connector,
        Err(_) => return Err(error::BenchmarkError::TlsError()),
    };

    let postgres_tls_connector = MakeTlsConnector::new(tls_connector);

    let (mut client, connection) =
        match tokio_postgres::connect(&database_url, postgres_tls_connector).await {
            Ok((client, connection)) => (client, connection),
            Err(e) => return Err(error::BenchmarkError::DatabaseError(e)),
        };

    // Spawn the connection future to drive the connection in the background
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Database connection error: {}", e);
        }
    });

    // Create tables if they don't exist
    match schema::create_tables(&mut client).await {
        Ok(_) => println!("Tables created successfully"),
        Err(e) => return Err(e),
    }

    // Now we can execute a simple statement that just returns its parameter.
    let rows = client.query("SELECT $1::TEXT", &[&"hello world"]).await?;

    // And then check that we got back the same string we sent over.
    let value: &str = rows[0].get(0);
    assert_eq!(value, "hello world");

    Ok(())
}
