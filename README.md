## Staking
- Find all current unspent boxes

- From current unspent boxes, find stake keys (R5.renderedValue where address=STAKE_ADDRESS and asset[0]=STAKE_KEY_ID)
.. this also provides the amount from assets[1], which is the tokenId (i.e. ERGOPAD_ID) and value
  > assets:
  >   1028de73d018f0c9a374b71555c5b8f1390994f2f41633e7b9d68f77735782ee: 1
  >   d71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413: 23858

- From found stake keys find where asset[0] is R5.renderedValue (above), this is the address
  > ergoTree: 0008cd03abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef
  > address: 9xyzpdqxyzpdqxyzpdqxyzpdqxyzpdqxyzpdqxyzpdqxyzpdqxyzpdq
  > assets:
  >   6fa205cd20a20a34ee888cd283e67c55296ddd2666ef67452d2b12da5e09ee62: 1

## SQL
```
# begin update with boxes to update is_spent
# partition table (list) on is_spent
# update starting from max(height)+1
# if unspent and last_seen height < current_height, invalid?
```
### boxes
. id
. box_id
. height
. is_spent
. last_seen (height?)

```
# using f'{NODE}/blocks/at/{i}'
# .. only need to check heights from unspent (boxes table)
```
### stake_keys
. id
. name (i.e. Ergopad/Paideia)
. token_id (ERGOPAD_ID)
. address (STAKE_ADDRESS)
. tree (STAKE_ADDRESS_TREE)
. key_id (STAKE_KEY_ID)

```
# only need to check heights from unspent (boxes table)
```
### stake_holders
. id
. address
. config_id (-> stake_config)
. box_id
. amount
. penalty
. last_seen (height?)


-- drop table audit_log
create table audit_log (
    id serial not null primary key,
    height int not null,
    created_at timestamp default now()
)

-- drop table boxes
create table boxes (
    id serial not null,
    box_id varchar(64) not null,
    height int not null,
    is_unspent boolean default true,
    primary key (id, is_unspent),
    unique (box_id, is_unspent)
);
partition by list(is_unspent);

create table boxes_spent partition of boxes for values in (false);
create table boxes_unspent partition of boxes for values in (true);

create table config (
    id serial primary key,
    table_name varchar(100) not null,
    parameter varchar(100) not null,
    value varchar(100) not null,
    description varchar(1000) null    
);

-- drop  table addresses_staking 
create table addresses_staking (
    id serial primary key,
    address text,
    token_id varchar(64) not null,
    box_id varchar(64) not null,
    amount bigint
);

-- drop table keys_staking 
create table keys_staking (
    id serial primary key,
    stakekey_token_id varchar(64),
    box_id varchar(64),
    token_id varchar(64),
    amount bigint,
    penalty bigint
);

-- drop table tokens
create table tokens (
    id serial primary key,
    token_name varchar(500),
    token_id varchar(64), -- ergopad [official token]
    token_type varchar(500), -- EIP-0004
    emission_amount bigint, -- 400M
    decimals int, -- 2
    stake_token_id varchar(64), -- ergopad Stake Token
    stake_ergotree varchar(2000)
);

insert into tokens (id, token_name, token_id, stake_token_id, stake_ergotree, decimals, token_type, emission_amount)
values (
    1,
    'ergopad',
    'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413', -- token_id
    '1028de73d018f0c9a374b71555c5b8f1390994f2f41633e7b9d68f77735782ee', -- stake_token_id
    '1017040004000e200549ea3374a36b7a22a803766af732e61798463c3332c5f6d86c8ab9195eed59040204000400040204020400040005020402040204060400040204040e2005cde13424a7972fbcd0b43fccbb5e501b1f75302175178fc86d8f243f3f312504020402010001010100d802d601b2a4730000d6028cb2db6308720173010001959372027302d80bd603b2a5dc0c1aa402a7730300d604e4c672030411d605e4c6a70411d606db63087203d607b27206730400d608db6308a7d609b27208730500d60ab27206730600d60bb27208730700d60c8c720b02d60de4c672010411d19683090193c17203c1a793c27203c2a793b272047308009ab27205730900730a93e4c67203050ee4c6a7050e93b27204730b00b27205730c00938c7207018c720901938c7207028c720902938c720a018c720b01938c720a029a720c9d9cb2720d730d00720cb2720d730e00d801d603b2a4730f009593c57203c5a7d801d604b2a5731000d1ed93720273119593c27204c2a7d801d605c67204050e95e67205ed93e47205e4c6a7050e938cb2db6308b2a573120073130001e4c67203050e73147315d17316',
    2,
    'EIP-004',
    40000000000
);

insert into tokens (id, token_name, token_id, stake_token_id, stake_ergotree, decimals, token_type, emission_amount)
values (
    2,
    'paideia',
    '1fd6e032e8476c4aa54c18c1a308dce83940e8f4a28f576440513ed7326ad489', -- token_id
    '245957934c20285ada547aa8f2c8e6f7637be86a1985b3e4c36e4e1ad8ce97ab', -- stake_token_id
    '101f040004000e2012bbef36eaa5e61b64d519196a1e8ebea360f18aba9b02d2a21b16f26208960f040204000400040001000e20b682ad9e8c56c5a0ba7fe2d3d9b2fbd40af989e8870628f4a03ae1022d36f0910402040004000402040204000400050204020402040604000100040404020402010001010100040201000100d807d601b2a4730000d6028cb2db6308720173010001d6039372027302d604e4c6a70411d605e4c6a7050ed60695ef7203ed93c5b2a4730300c5a78fb2e4c6b2a57304000411730500b2e4c6720104117306007307d6079372027308d1ecec957203d80ad608b2a5dc0c1aa402a7730900d609e4c672080411d60adb63087208d60bb2720a730a00d60cdb6308a7d60db2720c730b00d60eb2720a730c00d60fb2720c730d00d6107e8c720f0206d611e4c6720104119683090193c17208c1a793c27208c2a793b27209730e009ab27204730f00731093e4c67208050e720593b27209731100b27204731200938c720b018c720d01938c720b028c720d02938c720e018c720f01937e8c720e02069a72109d9c7eb272117313000672107eb27211731400067315957206d801d608b2a5731600ed72079593c27208c2a7d801d609c67208050e95e67209ed93e472097205938cb2db6308b2a57317007318000172057319731a731b9595efec7206720393c5b2a4731c00c5a7731d7207731e',
    4,
    'EIP-004',
    2000000000000
);