create table if not exists utxos
(
    id serial not null primary key,
    box_id varchar(64) not null unique,
    ergo_tree text,
    address varchar(64) not null,
    nergs bigint default 0,
    registers hstore,
    assets hstore,
    transaction_id varchar(64),
    creation_height integer,
    height integer not null,
);

create unique index on utxos (box_id);

