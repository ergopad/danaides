## QUICK START
> git clone https://github.com/ergo-pad/danaides.git<br>
> docker compose up<br>

## ENV
Minimally update when creating `.env` file:
> NODE_URL (ip address of ergonode)

Strongly recommended to update these, although build will technically work as-is.
> DANAIDES_PASSWORD (non-superuser, used in the application)
> POSTGRES_PASSWORD (superuser)
_The danaides password also needs to match what is in sql/init.sql_

# TL;DR
- Unspent boxes are stored in boxes table, including height; this is current list of all unspent boxes (spent are not currently saved)
- The boxes table needs to be constructed sequentially from height X to current height
- The utxos table is created by removing spent boxes (i.e. any box missing from boxes), and adding newly unspent boxes
- Assets, balances, vesting and staking are views of utxos; these may be pushed to tables and indexed for performance
- Tokens are stored in tokens tabls sequentially by height
- Tokenomics table is a view of tokens and utxo metrics including prices; likely stored in table for performance
- Simple logging is sent to audit_log; cleanup after 3 days

## REQUIREMENTS
- using postgres 14 _(now included; see manual steps to use existing database)_
- ergonode


## The Blah Blah...

## SQL
Understanding what's going on a bit here, or using an existing SQL server, be sure to create the danaides database and enable hstore datatypes:

```sql
-- should run this manually
create database danaides;
-- allow hstore columns; only need this once
create extension if not exists hstore;
```

## Performance
The primary goal of Danaides is to perform well for produciton.  This has caused some tweaky implementation steps, including some specific SQL code, which currently binds this implementation to Postgres (even though SqlAlchemy is used).
- In some scenarios, `docker network create ergopad-net` and binding all containers (including node) will improve performance of network requests
- The intermediate table creation uses a, "drop-n-pop" method, which creates a temp table, drops the primary/renames temp to avoid latency during the insert.  The drop/rename step is done in a transaction to avoid the race condition that would result in a missing table.  
<br>
_There are many opportunities to tune performance, but this project has been developed quickly so please submit suggestions to the ErgoPad Team._

## Permissions
Create the user that will perform all CRUD actions; all danaides operations
>`create user pirene with password xyzpdq;`

Update privileges.  Since tables are recreated for performance, the default privileges must be updated
>`alter default privileges in schema public grant select, insert, update, delete on tables to pirene;`<br>
>`alter default privileges in schema public grant usage on sequences to pirene;`

### NOTE: this may not be needed...
Since using drop-n-pop method, also initial creation of tables, this makes sense, although above permissions may be enough (TODO: clarify)
> `grant create on schema public to pirene;`

## First Run
_NOTE_: danaides_api is the container that will create the needed tables, not danaides so the dependencies are useful in the compose file<br>
<br>
From scratch, all tables and views will be created once you start docker compose.  This may be handy if changes are made and there is no clear path to sql migration, simply drop database and start over.

### Notes
- The API service builds the database objects, so must complete for danaides to run; restart if needed.
- The first run through will take some time to build boxes and then utxos table.
- Based on the method requesting node data, a local ergonode is required; async/multithreaded
- For performance, the ergonode (i.e. ergopad-quicknode, if using) should be on the docker network: ergopad-net
_(note: this is NOT how the default docker compose up.. works, but this method requires some manual configuration also)_

### Snapshots (TODO: in progress)
- all tables can be snapshot with naming convention: [table]_[height] (i.e. boxes_700000)

### CLI
- -H --height - begin at this height (0 will include genesis block)
- -V --verbose - be wordy
- -P --prettyprint - conserve node requests and wait polling to single line (lf, without cr)
- -B --override - process with just this box_id (use for testing)
- -O --once - process once and complete (don't wait for next block)

# Features (TODO: in progress)
- ?? Burn/Mint Tokens
- Integrate [Paideia Contracts](https://github.com/ergo-pad/paideia-contracts)
- API routes for coommon requests (i.e. staking, vesting), that are currently only available via SQL
- Setup partitioning
- .. goal: scalability
- .. utxos_X00 (for every 100k blocks)
- .. baseline performance since unspent boxes will stack closer to most recent; may need a better partition boundary
- .. does this jack with sqlalchemy metadata create_all?
- handle unicode in token names
