# INIT
`create database danaides;`

## Permissions
Create the user that will perform all CRUD actions; all danaides operations
>`create user pirene;`

Update privileges.  Since tables are recreated for performance, the default privileges must be updated
>`alter default privileges in schema public grant select, insert, update, delete on tables to pirene;`<br>
>`alter default privileges in schema public grant usage on sequences to pirene;`

## First Run
From scratch, all tables and views will be created.  This may be handy if changes are made and there is no clear path to sql migration, simply drop database and start over.

