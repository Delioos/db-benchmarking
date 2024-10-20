use chrono::{DateTime, Utc};
use futures::future::join_all;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Block {
    block_number: i32,
    block_hash: String,
    parent_hash: String,
    block_timestamp: DateTime<Utc>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Transaction {
    block: i32,
    index: i32,
    timestamp: DateTime<Utc>,
    hash: String,
    from: String,
    to: String,
    value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Transfer {
    tx_hash: String,
    block_number: i32,
    token: String,
    from: String,
    to: String,
    amount: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Pool {
    deployer: String,
    address: String,
    quote_token: String,
    token: String,
    init_block: i32,
    created_at: i64,
}

async fn create_tables(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS blocks (
            id SERIAL PRIMARY KEY,
            block_number INTEGER NOT NULL,
            block_hash TEXT NOT NULL,
            parent_hash TEXT NOT NULL,
            block_timestamp TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            block INTEGER NOT NULL,
            index INTEGER NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            hash TEXT NOT NULL,
            from_address TEXT NOT NULL,
            to_address TEXT NOT NULL,
            value TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS transfers (
            id SERIAL PRIMARY KEY,
            tx_hash TEXT NOT NULL,
            block_number INTEGER NOT NULL,
            token TEXT NOT NULL,
            from_address TEXT NOT NULL,
            to_address TEXT NOT NULL,
            amount TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS pools (
            id SERIAL PRIMARY KEY,
            deployer TEXT NOT NULL,
            address TEXT NOT NULL,
            quote_token TEXT NOT NULL,
            token TEXT NOT NULL,
            init_block INTEGER NOT NULL,
            created_at BIGINT NOT NULL
        )",
    )
    .execute(pool)
    .await?;

    Ok(())
}

async fn bulk_insert_test(
    pool: &PgPool,
    blocks: &[Block],
    transactions: &[Transaction],
    transfers: &[Transfer],
    pools: &[Pool],
) -> Result<f64, sqlx::Error> {
    let start = Instant::now();

    // Bulk insert blocks
    for chunk in blocks.chunks(1000) {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "INSERT INTO blocks (block_number, block_hash, parent_hash, block_timestamp, created_at, updated_at) "
        );

        query_builder.push_values(chunk, |mut b, block| {
            b.push_bind(block.block_number)
                .push_bind(&block.block_hash)
                .push_bind(&block.parent_hash)
                .push_bind(block.block_timestamp)
                .push_bind(block.created_at)
                .push_bind(block.updated_at);
        });

        query_builder.build().execute(pool).await?;
    }

    // Bulk insert transactions
    for chunk in transactions.chunks(1000) {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "INSERT INTO transactions (block, index, timestamp, hash, from_address, to_address, value) "
        );

        query_builder.push_values(chunk, |mut b, tx| {
            b.push_bind(tx.block)
                .push_bind(tx.index)
                .push_bind(tx.timestamp)
                .push_bind(&tx.hash)
                .push_bind(&tx.from)
                .push_bind(&tx.to)
                .push_bind(&tx.value);
        });

        query_builder.build().execute(pool).await?;
    }

    // Bulk insert transfers
    for chunk in transfers.chunks(1000) {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "INSERT INTO transfers (tx_hash, block_number, token, from_address, to_address, amount) "
        );

        query_builder.push_values(chunk, |mut b, transfer| {
            b.push_bind(&transfer.tx_hash)
                .push_bind(transfer.block_number)
                .push_bind(&transfer.token)
                .push_bind(&transfer.from)
                .push_bind(&transfer.to)
                .push_bind(&transfer.amount);
        });

        query_builder.build().execute(pool).await?;
    }

    // Bulk insert pools
    for chunk in pools.chunks(1000) {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "INSERT INTO pools (deployer, address, quote_token, token, init_block, created_at) ",
        );

        query_builder.push_values(chunk, |mut b, pool| {
            b.push_bind(&pool.deployer)
                .push_bind(&pool.address)
                .push_bind(&pool.quote_token)
                .push_bind(&pool.token)
                .push_bind(pool.init_block)
                .push_bind(pool.created_at);
        });

        query_builder.build().execute(pool).await?;
    }

    let duration = start.elapsed();
    let total_records = blocks.len() + transactions.len() + transfers.len() + pools.len();
    let rate = total_records as f64 / duration.as_secs_f64();

    Ok(rate)
}

async fn single_insert_test(
    pool: &PgPool,
    blocks: &[Block],
    transactions: &[Transaction],
    transfers: &[Transfer],
    pools: &[Pool],
) -> Result<f64, sqlx::Error> {
    let start = Instant::now();
    let mut rng = rand::thread_rng();

    for _ in 0..2500 {
        if let Some(block) = blocks.choose(&mut rng) {
            sqlx::query!(
                "INSERT INTO blocks (block_number, block_hash, parent_hash, block_timestamp, created_at, updated_at) 
                 VALUES ($1, $2, $3, $4, $5, $6)",
                block.block_number,
                block.block_hash,
                block.parent_hash,
                block.block_timestamp,
                block.created_at,
                block.updated_at
            )
            .execute(pool)
            .await?;
        }

        if let Some(tx) = transactions.choose(&mut rng) {
            sqlx::query!(
                "INSERT INTO transactions (block, index, timestamp, hash, from_address, to_address, value) 
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
                tx.block,
                tx.index,
                tx.timestamp,
                tx.hash,
                tx.from,
                tx.to,
                tx.value
            )
            .execute(pool)
            .await?;
        }

        if let Some(transfer) = transfers.choose(&mut rng) {
            sqlx::query!(
                "INSERT INTO transfers (tx_hash, block_number, token, from_address, to_address, amount) 
                 VALUES ($1, $2, $3, $4, $5, $6)",
                transfer.tx_hash,
                transfer.block_number,
                transfer.token,
                transfer.from,
                transfer.to,
                transfer.amount
            )
            .execute(pool)
            .await?;
        }

        if let Some(pool) = pools.choose(&mut rng) {
            sqlx::query!(
                "INSERT INTO pools (deployer, address, quote_token, token, init_block, created_at) 
                 VALUES ($1, $2, $3, $4, $5, $6)",
                pool.deployer,
                pool.address,
                pool.quote_token,
                pool.token,
                pool.init_block,
                pool.created_at
            )
            .execute(pool)
            .await?;
        }
    }

    let duration = start.elapsed();
    let rate = 10000.0 / duration.as_secs_f64();

    Ok(rate)
}

async fn read_write_mixed_workload_test(
    pool: &PgPool,
    blocks: &[Block],
    transactions: &[Transaction],
    transfers: &[Transfer],
    pools: &[Pool],
) -> Result<f64, sqlx::Error> {
    let start = Instant::now();
    let mut rng = rand::thread_rng();

    for _ in 0..50000 {
        let operation = rand::random::<f32>();

        if operation < 0.8 {
            // 80% write operations
            let data_type = rand::random::<f32>();
            if data_type < 0.25 {
                if let Some(block) = blocks.choose(&mut rng) {
                    sqlx::query!(
                        "INSERT INTO blocks (block_number, block_hash, parent_hash, block_timestamp, created_at, updated_at) 
                         VALUES ($1, $2, $3, $4, $5, $6)",
                        block.block_number,
                        block.block_hash,
                        block.parent_hash,
                        block.block_timestamp,
                        block.created_at,
                        block.updated_at
                    )
                    .execute(pool)
                    .await?;
                }
            } else if data_type < 0.5 {
                if let Some(tx) = transactions.choose(&mut rng) {
                    sqlx::query!(
                        "INSERT INTO transactions (block, index, timestamp, hash, from_address, to_address, value) 
                         VALUES ($1, $2, $3, $4, $5, $6, $7)",
                        tx.block,
                        tx.index,
                        tx.timestamp,
                        tx.hash,
                        tx.from,
                        tx.to,
                        tx.value
                    )
                    .execute(pool)
                    .await?;
                }
            } else if data_type < 0.75 {
                if let Some(transfer) = transfers.choose(&mut rng) {
                    sqlx::query!(
                        "INSERT INTO transfers (tx_hash, block_number, token, from_address, to_address, amount) 
                         VALUES ($1, $2, $3, $4, $5, $6)",
                        transfer.tx_hash,
                        transfer.block_number,
                        transfer.token,
                        transfer.from,
                        transfer.to,
                        transfer.amount
                    )
                    .execute(pool)
                    .await?;
                }
            } else {
                if let Some(pool_data) = pools.choose(&mut rng) {
                    sqlx::query!(
                        "INSERT INTO pools (deployer, address, quote_token, token, init_block, created_at) 
                         VALUES ($1, $2, $3, $4, $5, $6)",
                        pool_data.deployer,
                        pool_data.address,
                        pool_data.quote_token,
                        pool_data.token,
                        pool_data.init_block,
                        pool_data.created_at
                    )
                    .execute(pool)
                    .await?;
                }
            }
        } else {
            // 20% read operations
            let data_type = rand::random::<f32>();
            if data_type < 0.25 {
                sqlx::query!("SELECT * FROM blocks ORDER BY RANDOM() LIMIT 1")
                    .fetch_one(pool)
                    .await?;
            } else if data_type < 0.5 {
                sqlx::query!("SELECT * FROM transactions ORDER BY RANDOM() LIMIT 1")
                    .fetch_one(pool)
                    .await?;
            } else if data_type < 0.75 {
                sqlx::query!("SELECT * FROM transfers ORDER BY RANDOM() LIMIT 1")
                    .fetch_one(pool)
                    .await?;
            } else {
                sqlx::query!("SELECT * FROM pools ORDER BY RANDOM() LIMIT 1")
                    .fetch_one(pool)
                    .await?;
            }
        }
    }

    let duration = start.elapsed();
    let rate = 50000.0 / duration.as_secs_f64();

    Ok(rate)
}

async fn time_range_query_test(pool: &PgPool) -> Result<Vec<f64>, sqlx::Error> {
    let mut results = Vec::new();

    // 1 hour range
    let start = Instant::now();
    sqlx::query!("SELECT * FROM blocks WHERE block_timestamp >= NOW() - INTERVAL '1 hour'")
        .fetch_all(pool)
        .await?;
    let duration = start.elapsed();
    results.push(duration.as_secs_f64());

    // 1 day range
    let start = Instant::now();
    sqlx::query!("SELECT * FROM blocks WHERE block_timestamp >= NOW() - INTERVAL '1 day'")
        .fetch_all(pool)
        .await?;
    let duration = start.elapsed();
    results.push(duration.as_secs_f64());

    // 1 week range
    let start = Instant::now();
    sqlx::query!("SELECT * FROM blocks WHERE block_timestamp >= NOW() - INTERVAL '1 week'")
        .fetch_all(pool)
        .await?;
    let duration = start.elapsed();
    results.push(duration.as_secs_f64());

    Ok(results)
}

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let DATABASE_URL = "zeub";
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://username:password@localhost/blockchain_db")
        .await?;

    create_tables(&pool).await?;

    // Load test data
    let blocks: Vec<Block> = serde_json::from_str(
        &std::fs::read_to_string("../../data/blocks.json").expect("Failed to read blocks.json"),
    )
    .expect("Failed to parse JSON");

    let transactions: Vec<Transaction> = serde_json::from_str(
        &std::fs::read_to_string("../../data/transactions.json")
            .expect("Failed to read transactions.json"),
    )
    .expect("Failed to parse JSON");

    let transfers: Vec<Transfer> = serde_json::from_str(
        &std::fs::read_to_string("../../data/transfers.json")
            .expect("Failed to read transfers.json"),
    )
    .expect("Failed to parse JSON");

    let pools: Vec<Pool> = serde_json::from_str(
        &std::fs::read_to_string("../../data/pools.json").expect("Failed to read pools.json"),
    )
    .expect("Failed to parse JSON");

    // Run tests
    println!("Starting benchmark tests...");

    // Bulk Insert Test
    let bulk_insert_rate = bulk_insert_test(
        &pool,
        &blocks[..100000],
        &transactions[..100000],
        &transfers[..100000],
        &pools[..100000],
    )
    .await?;
    println!("Bulk insert rate: {:.2} records/second", bulk_insert_rate);

    // Clear tables for next test
    sqlx::query("TRUNCATE blocks, transactions, transfers, pools")
        .execute(&pool)
        .await?;

    // Single Insert Test
    let single_insert_rate =
        single_insert_test(&pool, &blocks, &transactions, &transfers, &pools).await?;
    println!(
        "Single insert rate: {:.2} records/second",
        single_insert_rate
    );

    // Clear tables for next test
    sqlx::query("TRUNCATE blocks, transactions, transfers, pools")
        .execute(&pool)
        .await?;

    // Read-Write Mixed Workload Test
    let mixed_workload_rate =
        read_write_mixed_workload_test(&pool, &blocks, &transactions, &transfers, &pools).await?;
    println!(
        "Read-write mixed workload rate: {:.2} operations/second",
        mixed_workload_rate
    );

    // Time-Range Query Test
    let time_range_results = time_range_query_test(&pool).await?;
    println!("Time-range query results:");
    println!("  1 hour range: {:.2} seconds", time_range_results[0]);
    println!("  1 day range: {:.2} seconds", time_range_results[1]);
    println!("  1 week range: {:.2} seconds", time_range_results[2]);

    // Additional metrics (these would typically be collected throughout the benchmark)
    // For demonstration, we're just printing placeholders
    println!("\nAdditional Metrics (placeholders):");
    println!("CPU Usage: XX%");
    println!("Memory Usage: XX MB");
    println!("Disk I/O: XX MB/s");

    println!("\nBenchmark tests completed.");

    Ok(())
}
