use dotenv::dotenv;
use futures::pin_mut;
use native_tls::TlsConnector;
use postgres::types::ToSql;
use postgres_native_tls::MakeTlsConnector;
use serde::Deserialize;
use serde_json;
use std::env;
use std::fs::File;
use std::time::Instant;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::Type;
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
    // Now we can execute a simple statement that just returns its parameter.
    let rows = client.query("SELECT $1::TEXT", &[&"hello world"]).await?;

    // And then check that we got back the same string we sent over.
    let value: &str = rows[0].get(0);
    assert_eq!(value, "hello world");

    // Create tables if they don't exist
    match schema::create_tables(&mut client).await {
        Ok(_) => println!("Tables created successfully"),
        Err(e) => return Err(e),
    }

    let start = Instant::now();
    // Load blocks from JSON file
    // let prefix: &str = "../../data/S"; // remove the S to x100 the data size
    let prefix: &str = "../../data/";
    let blocks: Vec<models::Block> = match load_json_data(&format!("{}blocks.json", prefix)) {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Error loading blocks: {}", e);
            Vec::new()
        }
    };

    // Load pools from JSON file
    let pools: Vec<models::Pool> = match load_json_data(&format!("{}pools.json", prefix)) {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Error loading pools: {}", e);
            Vec::new()
        }
    };

    // Load transactions from JSON file
    let transactions: Vec<models::Transaction> =
        match load_json_data(&format!("{}transactions.json", prefix)) {
            Ok(data) => data,
            Err(e) => {
                eprintln!("Error loading transactions: {}", e);
                Vec::new()
            }
        };

    // Load transfers from JSON file
    let transfers: Vec<models::Transfer> =
        match load_json_data(&format!("{}transfers.json", prefix)) {
            Ok(data) => data,
            Err(e) => {
                eprintln!("Error loading transfers: {}", e);
                Vec::new()
            }
        };
    let duration = start.elapsed();
    // Print the loaded data
    println!("Loaded {} blocks", blocks.len());
    println!("Loaded {} transactions", transactions.len());
    println!("Loaded {} transfers", transfers.len());
    println!("Loaded {} pools", pools.len());
    println!("in {:?}", duration);

    // backtest

    // 1. Bulk Insert Test
    let start = Instant::now();
    let total_records = blocks.len();
    let mut batch_size = total_records / 100;
    if batch_size == 0 {
        batch_size = 1;
    }
    let num_batches = (total_records + batch_size - 1) / batch_size;

    println!("\nStarting Bulk Insert Tests:");
    println!("Total batches: {}", num_batches);
    println!("Batch size: {}", batch_size);

    for i in 0..num_batches {
        let start_index = i * batch_size;
        let end_index = std::cmp::min((i + 1) * batch_size, total_records);

        // Get batches for each type
        let block_batch = &blocks[start_index..end_index];
        let transaction_batch =
            &transactions[start_index..std::cmp::min(end_index, transactions.len())];
        let transfer_batch = &transfers[start_index..std::cmp::min(end_index, transfers.len())];
        let pool_batch = &pools[start_index..std::cmp::min(end_index, pools.len())];

        // 1. Bulk insert blocks
        let sink = client
            .copy_in("COPY blocks (block_number, block_hash, parent_hash, block_timestamp, created_at, updated_at) FROM STDIN BINARY")
            .await?;
        let types = &[
            Type::INT4,
            Type::TEXT,
            Type::TEXT,
            Type::TEXT,
            Type::TEXT,
            Type::TEXT,
        ];
        let writer = BinaryCopyInWriter::new(sink, types);
        pin_mut!(writer);

        for block in block_batch {
            writer
                .as_mut()
                .write(&[
                    &block.block_number as &(dyn ToSql + Sync),
                    &block.block_hash.as_str() as &(dyn ToSql + Sync),
                    &block.parent_hash.as_str() as &(dyn ToSql + Sync),
                    &block.block_timestamp.as_str() as &(dyn ToSql + Sync),
                    &block.created_at.as_str() as &(dyn ToSql + Sync),
                    &block.updated_at.as_str() as &(dyn ToSql + Sync),
                ])
                .await?;
        }
        writer.as_mut().finish().await?;

        // 2. Bulk insert transactions
        let sink = client
            .copy_in("COPY transactions (block, index, timestamp, hash, from_address, to_address, value) FROM STDIN BINARY")
            .await?;
        let types = &[
            Type::INT4,
            Type::INT4,
            Type::TEXT,
            Type::TEXT,
            Type::TEXT,
            Type::TEXT,
            Type::TEXT,
        ];
        let writer = BinaryCopyInWriter::new(sink, types);
        pin_mut!(writer);

        for tx in transaction_batch {
            writer
                .as_mut()
                .write(&[
                    &tx.block as &(dyn ToSql + Sync),
                    &tx.index as &(dyn ToSql + Sync),
                    &tx.timestamp.as_str() as &(dyn ToSql + Sync),
                    &tx.hash.as_str() as &(dyn ToSql + Sync),
                    &tx.from.as_str() as &(dyn ToSql + Sync),
                    &tx.to.as_str() as &(dyn ToSql + Sync),
                    &tx.value.as_str() as &(dyn ToSql + Sync),
                ])
                .await?;
        }
        writer.as_mut().finish().await?;

        // 3. Bulk insert transfers
        let sink = client
            .copy_in("COPY transfers (tx_hash, block_number, token, from_address, to_address, amount) FROM STDIN BINARY")
            .await?;
        let types = &[
            Type::TEXT,
            Type::INT4,
            Type::TEXT,
            Type::TEXT,
            Type::TEXT,
            Type::TEXT,
        ];
        let writer = BinaryCopyInWriter::new(sink, types);
        pin_mut!(writer);

        for transfer in transfer_batch {
            writer
                .as_mut()
                .write(&[
                    &transfer.tx_hash.as_str() as &(dyn ToSql + Sync),
                    &transfer.block_number as &(dyn ToSql + Sync),
                    &transfer.token.as_str() as &(dyn ToSql + Sync),
                    &transfer.from.as_str() as &(dyn ToSql + Sync),
                    &transfer.to.as_str() as &(dyn ToSql + Sync),
                    &transfer.amount.as_str() as &(dyn ToSql + Sync),
                ])
                .await?;
        }
        writer.as_mut().finish().await?;

        // 4. Bulk insert pools
        let sink = client
            .copy_in("COPY pools (deployer, address, quote_token, token, init_block, created_at) FROM STDIN BINARY")
            .await?;
        let types = &[
            Type::TEXT,
            Type::TEXT,
            Type::TEXT,
            Type::TEXT,
            Type::INT4,
            Type::INT8,
        ];
        let writer = BinaryCopyInWriter::new(sink, types);
        pin_mut!(writer);

        for pool in pool_batch {
            writer
                .as_mut()
                .write(&[
                    &pool.deployer.as_str() as &(dyn ToSql + Sync),
                    &pool.address.as_str() as &(dyn ToSql + Sync),
                    &pool.quote_token.as_str() as &(dyn ToSql + Sync),
                    &pool.token.as_str() as &(dyn ToSql + Sync),
                    &pool.init_block as &(dyn ToSql + Sync),
                    &pool.created_at as &(dyn ToSql + Sync),
                ])
                .await?;
        }
        writer.as_mut().finish().await?;

        if i % 10 == 0 || i == num_batches - 1 {
            println!("Processed batch {}/{}", i + 1, num_batches);
        }
    }

    let bulk_insert_duration = start.elapsed();
    println!("\nBulk Insert Test Results:");
    println!("-------------------------");
    println!("Total records processed:");
    println!("  Blocks: {}", blocks.len());
    println!("  Transactions: {}", transactions.len());
    println!("  Transfers: {}", transfers.len());
    println!("  Pools: {}", pools.len());
    println!("Total duration: {:?}", bulk_insert_duration);
    println!(
        "Average insertion rate: {} records/sec",
        (blocks.len() + transactions.len() + transfers.len() + pools.len()) as f64
            / bulk_insert_duration.as_secs_f64()
    );

    // 2. Single Record Insert Test
    // ...

    // 3. Read-Write Mixed Workload Test
    // ...

    // 4. Time-Range Query Test
    // ...

    Ok(())
}
