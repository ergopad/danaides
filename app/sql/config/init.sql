-- database user (this should NOT be dba)
do
$$
begin
  if not exists (select * from pg_user where usename = 'pirene') then 
     create user pirene with password 'pirene';
  end if;
end
$$
;

-- allow hstore datatypes
create extension if not exists hstore;

-- init objcects
create schema if not exists checkpoint;
-- allow create table, drop/replace table and create index
grant all privileges on schema checkpoint to pirene;

create table if not exists alembic_version (
    version_num varchar(32) not null
);

-- perms
grant all privileges on database danaides to pirene; -- ?? this seems a bit much
grant connect on database danaides TO pirene;

alter default privileges in schema public grant select, insert, update, delete on tables to pirene;
alter default privileges in schema public grant usage on sequences to pirene;

grant create on schema public to pirene;
