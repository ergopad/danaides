create table if not exists audit_log
(
    id serial not null primary key,
    height integer not null,
    created_at timestamp without time zone default now(),
    service varchar(64) default 'main'
    notes text
)
