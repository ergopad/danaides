create table if not exists tokens_meta
(
    id serial not null primary key,
    token_name varchar(500),
    token_id varchar(64) unique,
    token_type varchar(500),
    emission_amount bigint,
    decimals integer,
    stake_token_id varchar(64),
    stake_ergotree varchar(2000),
    current_total_supply bigint,
    in_circulation bigint,
    token_price numeric(32,10),
    vested double precision default 0.0,
    emitted double precision default 0.0,
    staked double precision default 0.0,
    stake_pool double precision default 0.0
)
