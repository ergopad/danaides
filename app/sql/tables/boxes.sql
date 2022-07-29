create table boxes (
    id serial not null primary key,
    box_id varchar(64),
    height int, 
    is_unspent bool,
    nerg bigint
);
