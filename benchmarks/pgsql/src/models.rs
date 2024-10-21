use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    pub block_number: i32,
    pub block_hash: String,
    pub parent_hash: String,
    pub block_timestamp: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub block: i32,
    pub index: i32,
    pub timestamp: DateTime<Utc>,
    pub hash: String,
    pub from: String,
    pub to: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transfer {
    pub tx_hash: String,
    pub block_number: i32,
    pub token: String,
    pub from: String,
    pub to: String,
    pub amount: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pool {
    pub deployer: String,
    pub address: String,
    pub quote_token: String,
    pub token: String,
    pub init_block: i32,
    pub created_at: i64,
}
