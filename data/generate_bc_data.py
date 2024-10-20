import random
from datetime import datetime, timedelta
from typing import List, Dict
import json
from tqdm import tqdm

# Constants
NUM_BLOCKS = 1000000
TRANSACTIONS_PER_BLOCK = 10
TRANSFERS_PER_BLOCK = 5
POOLS_PER_BLOCK = 2
START_DATE = datetime(2023, 1, 1)

def generate_address() -> str:
    return '0x' + ''.join(random.choices('0123456789abcdef', k=40))

def generate_hash() -> str:
    return '0x' + ''.join(random.choices('0123456789abcdef', k=64))

def generate_blocks() -> List[Dict]:
    blocks = []
    for i in tqdm(range(NUM_BLOCKS), desc="Generating blocks"):
        block_time = START_DATE + timedelta(seconds=i*15)  # Assuming 15-second block time
        block = {
            "block_number": i,
            "block_hash": generate_hash(),
            "parent_hash": generate_hash(),
            "block_timestamp": block_time.isoformat(),
            "created_at": block_time.isoformat(),
            "updated_at": block_time.isoformat()
        }
        blocks.append(block)
    return blocks

def generate_transactions(blocks: List[Dict]) -> List[Dict]:
    transactions = []
    for block in tqdm(blocks, desc="Generating transactions"):
        for _ in range(TRANSACTIONS_PER_BLOCK):
            tx = {
                "block": block["block_number"],
                "index": len(transactions) % 256,
                "timestamp": block["block_timestamp"],
                "hash": generate_hash(),
                "from": generate_address(),
                "to": generate_address(),
                "value": str(random.randint(0, 10**18))
            }
            transactions.append(tx)
    return transactions

def generate_transfers(blocks: List[Dict]) -> List[Dict]:
    transfers = []
    for block in tqdm(blocks, desc="Generating transfers"):
        for _ in range(TRANSFERS_PER_BLOCK):
            transfer = {
                "tx_hash": generate_hash(),
                "block_number": block["block_number"],
                "token": generate_address(),
                "from": generate_address(),
                "to": generate_address(),
                "amount": str(random.randint(0, 10**18))
            }
            transfers.append(transfer)
    return transfers

def generate_pools(blocks: List[Dict]) -> List[Dict]:
    pools = []
    for block in tqdm(blocks, desc="Generating pools"):
        for _ in range(POOLS_PER_BLOCK):
            pool = {
                "deployer": generate_address(),
                "address": generate_address(),
                "quote_token": generate_address(),
                "token": generate_address(),
                "init_block": block["block_number"],
                "created_at": int(datetime.fromisoformat(block["block_timestamp"]).timestamp())
            }
            pools.append(pool)
    return pools

def main():
    print("Generating blockchain data...")
    blocks = generate_blocks()
    transactions = generate_transactions(blocks)
    transfers = generate_transfers(blocks)
    pools = generate_pools(blocks)

    print("Saving data to files...")
    with tqdm(total=4, desc="Saving files") as pbar:
        with open('blocks.json', 'w') as f:
            json.dump(blocks, f)
        pbar.update(1)
        
        with open('transactions.json', 'w') as f:
            json.dump(transactions, f)
        pbar.update(1)
        
        with open('transfers.json', 'w') as f:
            json.dump(transfers, f)
        pbar.update(1)
        
        with open('pools.json', 'w') as f:
            json.dump(pools, f)
        pbar.update(1)

    print(f"Generated {len(blocks)} blocks")
    print(f"Generated {len(transactions)} transactions")
    print(f"Generated {len(transfers)} transfers")
    print(f"Generated {len(pools)} pools")
    print("Data generation complete.")

if __name__ == "__main__":
    main()
