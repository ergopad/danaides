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

-- perms
grant all privileges on database danaides to pirene;
grant connect on database danaides TO pirene;

alter default privileges in schema public grant select, insert, update, delete on tables to pirene;
alter default privileges in schema public grant usage on sequences to pirene;

grant create on schema public to pirene;

-- useful for healthcheck and not returning null rows
create table if not exists audit_log (
    id serial primary key not null,
    height int not null,
    created_at timestamp without time zone default now(),
    service varchar(64),
    notes text
);
do
$$
begin
    if not exists (select * from audit_log where height = -1) then
        insert into audit_log (height, service, notes)
        values (-1, 'init', 'created by postgres container startup');
    end if;
end
$$
;
