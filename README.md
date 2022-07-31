# TL;DR
- Unspent boxes are stored in boxes table, including height; this is current list of all unspent boxes (spent are not currently saved)
- The boxes table needs to be constructed sequentially from height X to current height
- The utxos table is created by removing spent boxes (i.e. any box missing from boxes), and adding newly unspent boxes
- Assets, balances, vesting and staking are views of utxos; these may be pushed to tables and indexed for performance
- Tokens are stored in tokens tabls sequentially by height
- Tokenomics table is a view of tokens and utxo metrics including prices; likely stored in table for performance
- Simple logging is sent to audit_log; cleanup after 3 days

## REQUIREMENTS
- using postgres 14
- ergonode

## QUICK START
> git clone https://github.com/ergo-pad/danaides.git<br>
> docker compose up<br>

## SQL
```sql
-- should run this manually
create database danaides;
-- allow hstore columns; only need this once
create extension if not exists hstore;
```

- set .env for container (in compose.yml, or include .env)
```
    environment:
      POSTGRES_USER=danaides
      POSTGRES_PASSWORD=supersecret
      POSTGRES_HOST=localhost
      POSTGRES_PORT=5432
      POSTGRES_DBNM=danaides
      NODE_URL=quicknode
      NODE_PORT=9053
```

### Notes
- The API service builds the database objects, so must complete for danaides to run; restart if needed.
- The first run through will take some time to build boxes and then utxos table.
- Based on the method requesting node data, a local ergonode is required; async/multithreaded
- the ergonode (i.e. quicknode, if using), should be on the docker network: ergopad-net

### Snapshots
- all tables can be snapshot with naming convention: [table]_[height] (i.e. boxes_700000)

### CLI
- -H --height - begin at this height (0 will include genesis block)
- -V --verbose - be wordy
- -P --prettyprint - conserve node requests and wait polling to single line (lf, without cr)
- -B --override - process with just this box_id (use for testing)
- -O --once - process once and complete (don't wait for next block)
