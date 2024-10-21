use dotenv::dotenv;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use serde::Deserialize;
use serde_json;
use std::env;
use std::fs::File;
use tokio_postgres::{Client, NoTls};

mod error;
mod models;
mod schema;

fn load_json_data<T>(file_path: &str) -> Result<Vec<T>, serde_json::Error>
where
    T: for<'a> Deserialize<'a>,
{
    let file = File::open(file_path).unwrap();
    serde_json::from_reader(file)
}

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

    // Load blocks from JSON file
    let blocks: Vec<models::Block> = match load_json_data("../../data/blocks.json") {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Error loading blocks: {}", e);
            Vec::new()
        }
    };

    // Load transactions from JSON file
    let transactions: Vec<models::Transaction> =
        match load_json_data("../../data/transactions.json") {
            Ok(data) => data,
            Err(e) => {
                eprintln!("Error loading transactions: {}", e);
                Vec::new()
            }
        };

    // Load transfers from JSON file
    let transfers: Vec<models::Transfer> = match load_json_data("../../data/transfers.json") {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Error loading transfers: {}", e);
            Vec::new()
        }
    };

    // Load pools from JSON file
    let pools: Vec<models::Pool> = match load_json_data("../../data/pools.json") {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Error loading pools: {}", e);
            Vec::new()
        }
    };

    // Print the loaded data
    println!("Loaded {} blocks", blocks.len());
    println!("Loaded {} transactions", transactions.len());
    println!("Loaded {} transfers", transfers.len());
    println!("Loaded {} pools", pools.len());

    Ok(())
}
