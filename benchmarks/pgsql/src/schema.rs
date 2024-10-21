use crate::error::{BenchmarkError, Result};
use postgres::{Client, NoTls};

pub async fn create_tables(client: &mut Client) -> Result<()> {
    client.execute(
        "CREATE TABLE IF NOT EXISTS blocks (
            id SERIAL PRIMARY KEY,
            block_number INTEGER NOT NULL,
            block_hash TEXT NOT NULL,
            parent_hash TEXT NOT NULL,
            block_timestamp TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        )",
        &[],
    )?;

    client.execute(
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
        &[],
    )?;

    client.execute(
        "CREATE TABLE IF NOT EXISTS transfers (
            id SERIAL PRIMARY KEY,
            tx_hash TEXT NOT NULL,
            block_number INTEGER NOT NULL,
            token TEXT NOT NULL,
            from_address TEXT NOT NULL,
            to_address TEXT NOT NULL,
            amount TEXT NOT NULL
        )",
        &[],
    )?;

    client.execute(
        "CREATE TABLE IF NOT EXISTS s (
            id SERIAL PRIMARY KEY,
            deployer TEXT NOT NULL,
            address TEXT NOT NULL,
            quote_token TEXT NOT NULL,
            token TEXT NOT NULL,
            init_block INTEGER NOT NULL,
            created_at BIGINT NOT NULL
        )",
        &[],
    )?;

    Ok(())
}
