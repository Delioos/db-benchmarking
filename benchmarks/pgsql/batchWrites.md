postgresql using native_tls and binary copy write throughput for blockchain data batch processing : 

```
  creating tables...
Tables created successfully
Loaded 1000000 blocks
Loaded 10000000 transactions
Loaded 5000000 transfers
Loaded 2000000 pools
in 3013.799281091s

Starting Bulk Insert Tests:
Total batches: 100
Batch size: 10000
Processed batch 1/100
Processed batch 11/100
Processed batch 21/100
Processed batch 31/100
Processed batch 41/100
Processed batch 51/100
Processed batch 61/100
Processed batch 71/100
Processed batch 81/100
Processed batch 91/100
Processed batch 100/100

Bulk Insert Test Results:
-------------------------
Total records processed:
  Blocks: 1000000
  Transactions: 10000000
  Transfers: 5000000
  Pools: 2000000
Total duration: 107.693625672s
Average insertion rate: 167140.81160961362 records/sec
```

comfy comfy 

kudos to @joshstevens19 for the rindexer pgsql client inspiration
