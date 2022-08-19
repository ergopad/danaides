# INIT
> `create database danaides;`
> `create extension if not exists hstore;`

## Permissions
Create the user that will perform all CRUD actions; all danaides operations
>`create user pirene;`

Update privileges.  Since tables are recreated for performance, the default privileges must be updated
>`alter default privileges in schema public grant select, insert, update, delete on tables to pirene;`<br>
>`alter default privileges in schema public grant usage on sequences to pirene;`
### may not be needed
Since using drop-n-pop method, also initial creation of tables, this makes sense, although above permissions may be enough (TODO: clarify)
> `grant create on schema public to pirene;`

## First Run
_NOTE_: danaides_api is the container that will create the needed tables, not danaides<br>
<br>
From scratch, all tables and views will be created.  This may be handy if changes are made and there is no clear path to sql migration, simply drop database and start over.
