create table boxes_654321 (
    id serial not null primary key,
    box_id varchar(64),
    height int, 
    is_unspent bool,
    nerg bigint
);

-- populate boxes
select max(height) from boxes

-- to restart, 
insert into boxes
    select * from boxes_654321

-- make sure the processor can figure this out
insert into audit_log (height, service) values (654321, 'boxes')
