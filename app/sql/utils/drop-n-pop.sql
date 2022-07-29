-- TODO: need check for basic things like blank table?

-- perform in transactions to allow rollback;
create table tmp_staking (
                id serial not null primary key,
                address varchar(64),
                token_id varchar(64),
                box_id varchar(64),
                stakekey_token_id varchar(64),
                amount bigint,
                penalty varchar(64)
            );
insert into tmp_staking (address, token_id, box_id, stakekey_token_id, amount, penalty)
    select address, token_id, box_id, stakekey_token_id, amount, penalty from v_staking;

drop table staking;

alter table tmp_staking rename to staking;
