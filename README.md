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

## Permissions
Create the user that will perform all CRUD actions; all danaides operations
>`create user pirene;`

Update privileges.  Since tables are recreated for performance, the default privileges must be updated
>`alter default privileges in schema public grant select, insert, update, delete on tables to pirene;`<br>
>`alter default privileges in schema public grant usage on sequences to pirene;`
### may not be needed
Since using drop-n-pop method, also initial creation of tables, this makes sense, although above permissions may be enough (TODO: clarify)
> `grant create on schema public to pirene;`

## ENV
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

## First Run
_NOTE_: danaides_api is the container that will create the needed tables, not danaides<br>
<br>
From scratch, all tables and views will be created.  This may be handy if changes are made and there is no clear path to sql migration, simply drop database and start over.

### Notes
- The API service builds the database objects, so must complete for danaides to run; restart if needed.
- The first run through will take some time to build boxes and then utxos table.
- Based on the method requesting node data, a local ergonode is required; async/multithreaded
- the ergonode (i.e. quicknode, if using), should be on the docker network: ergopad-net

### Snapshots (TODO: in progress)
- all tables can be snapshot with naming convention: [table]_[height] (i.e. boxes_700000)

### CLI
- -H --height - begin at this height (0 will include genesis block)
- -V --verbose - be wordy
- -P --prettyprint - conserve node requests and wait polling to single line (lf, without cr)
- -B --override - process with just this box_id (use for testing)
- -O --once - process once and complete (don't wait for next block)

# Features (TODO: in progress)
- Burn/Mint Tokens
- Integrate [Paideia Contracts](https://github.com/ergo-pad/paideia-contracts)
- API routes for coommon requests (i.e. staking, vesting), that are currently only available via SQL
- Setup partitioning
- .. goal: scalability
- .. utxos_X00 (for every 100k blocks)
- .. baseline performance since unspent boxes will stack closer to most recent; may need a better partition boundary
- .. does this jack with sqlalchemy metadata create_all?
- handle unicode in token names
