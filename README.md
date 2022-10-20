# QUICK START
> git clone https://github.com/ergo-pad/danaides.git<br>
> docker compose up<br>

## ENV
Minimally update when creating `.env` file:<br>
> NODE_URL (ip address of ergonode)<br>

Strongly recommended to update these, although build will technically work as-is.
> DANAIDES_PASSWORD (non-superuser, used in the application)<br>
> POSTGRES_PASSWORD (superuser)<br>
_The danaides password also needs to match what is in sql/init.sql_<br>

<br><hr><br>

# ALT START
To use a custom config easily, danaides.yml can be setup<br>
> docker compose -f danaides.yml up<br>
Change the POSTGRES_HOST in .env to use the proper database<br>
<br>
_optionally,_<br>
> docker network create ergopad-net<br>

<br><hr><br>

# TL;DR
- Unspent boxes are stored in `boxes` and `utxos` tables; this is current list of all unspent boxes (spent are not currently saved)
- The boxes table needs to be constructed sequentially from height X to current height
- The utxos table is created by removing spent boxes (i.e. any box missing from boxes), and adding newly unspent boxes
- Assets, balances, vesting and staking are views of utxos; these are materialized views and updated concurrently for performance and minimizing downtime
- Tokens are stored in tokens table sequentially by height
- Tokenomics tables are views of tokens and utxo metrics including prices; likely stored in table for performance
- Simple logging is sent to audit_log; cleanup after 3 days

## REQUIREMENTS
- using postgres 14
- ergonode

## OPTIONAL
- postgres is included, or an existing version can be used; _note: the hstore extension is required_
- a celery folder is added to allow offloading processing of materialized views

<br>
<hr>
<br>

# The Blah Blah...

## SQL
A startup script, app/sql/config/init.sql can be used to get going quickly or act as a reference.  Be sure to change the default password for the user, pirene.

### Or, manually...
```sql
create database danaides;
-- allow hstore columns; only need this once
create extension if not exists hstore;
```

## Performance
The primary goal of Danaides is to perform well for production.
- In some scenarios, `docker network create ergopad-net` and binding all containers (including node) will improve performance.
- Materialized views are refreshed concurrently, which is slower than normal but does not block.
- There are some monitoring tools in the celery folder, which can be helpful for monitoring performance.

## Permissions
If using own database, create the user that will perform all CRUD actions; all danaides operations<br>
>`create user pirene with password xyzpdq;`<br>

Update privileges.  Since tables are recreated for performance, the default privileges must be updated<br>
>`alter default privileges in schema public grant select, insert, update, delete on tables to pirene;`<br>
>`alter default privileges in schema public grant usage on sequences to pirene;`<br>

### NOTE: this may not be needed...
Since using drop-n-pop method, also initial creation of tables, this makes sense, although above permissions may be enough (TODO: clarify)<br>
> `grant create on schema public to pirene;`<br>

## API
The API is used to extract data from the Danaides database in JSON format.  It is also used to refresh the materizlized views and maintain some database integrity during startup, therefore is important to the normal workflow.<br>
<br>
_Note: early versions of Danaides did not depend on the api container, like now._

<br><hr><br>

# SQL Migrations

This section is a work in progress.  The schema has been developed in SqlAlchemy, and is not being converted to used alembic so that version can be properly migrated.  Currently this section is a drop spot for notes and not currently used.<br>
<br>
To perform these operations, the d-alembic container can be used by referncing the profile:
> `docker exec -it danaides-alembic bash`

## Common Operations

> `alembic init alembic`<br>
<br>

Update env.py<br>
> `import sqlalchemy.ext.declarative as dec`
> `from os import getenv`
> `config.set_main_option("sqlalchemy.url", f"postgresql://{getenv('POSTGRES_USER')}:{getenv('POSTGRES_PASSWORD')}@{getenv('POSTGRES_HOST')}:{getenv('POSTGRES_PORT')}/{getenv('POSTGRES_DB')}")`
> `SqlAlchemyBase = dec.declarative_base()`
> `target_metadata = SqlAlchemyBase.metadata`

### Create Intial Migration
> `alembic revision --autogenerate -m "Initial migration."`
> `alembic upgrade head`

### Create More Migrations
> `alembic revision --autogenerate -m "Added new table."`
> `?? alembic upgrade head`

<br><hr><br>

# Notes
- The API service builds the database objects, so must complete for danaides to run; restart if needed.
- The first run through will take some time to build boxes, then utxos tables.  This only needs to happen once.
- Based on the method requesting node data, a local ergonode is required; async/multithreaded
- For performance, the ergonode (i.e. ergopad-quicknode, if using) should be on the docker network: ergopad-net
_(note: this is NOT how the default docker compose up.. works, but this method may require manual configuration also)_

### Snapshots (TODO: in progress)
_This feature is not yet implemented_
- all tables can be snapshot with naming convention: [table]_[height] (i.e. boxes_700000)

### CLI
These are not typically used unless using `docker exec` to run danaides
- -H --height - begin at this height (0 will include genesis block)
- -V --verbose - be wordy
- -P --prettyprint - conserve node requests and wait polling to single line (lf, without cr)
- -B --override - process with just this box_id (use for testing)
- -O --once - process once and complete (don't wait for next block)

<br><hr><br>

# Features (TODO: in progress)
- ?? Burn/Mint Tokens - this is currently not high on the priority list
- Integrate [Paideia Contracts](https://github.com/ergo-pad/paideia-contracts)
- API routes for coommon requests (i.e. staking, vesting), that are currently only available via SQL
- ?? Setup partitioning
- .. goal: scalability/performance
- .. utxos_X00 (for every 100k blocks)
- .. baseline performance since unspent boxes will stack closer to most recent; may need a better partition boundary
- !! handle unicode in token names
