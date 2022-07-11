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
```sql
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
    service varchar(64) not null default 'main',
    notes text null,
    created_at timestamp default now()
)

-- drop table boxes
create table boxes (
    id serial not null,
    box_id varchar(64) not null,
    height int not null,
    nerg bigint,
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
    stake_ergotree varchar(2000),
    current_total_supply bigint,
    in_circulation bigint,
    token_price numeric(20, 10)
);

-- use /utils/addressToRaw/{address} on the smart contract to get stake_ergotree
insert into tokens (id, token_name, token_id, stake_token_id, stake_ergotree, decimals, token_type, emission_amount)
values (
    1,
    'ErgoPad',
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
    'Paideia',
    '1fd6e032e8476c4aa54c18c1a308dce83940e8f4a28f576440513ed7326ad489', -- token_id
    '245957934c20285ada547aa8f2c8e6f7637be86a1985b3e4c36e4e1ad8ce97ab', -- stake_token_id
    '101f040004000e2012bbef36eaa5e61b64d519196a1e8ebea360f18aba9b02d2a21b16f26208960f040204000400040001000e20b682ad9e8c56c5a0ba7fe2d3d9b2fbd40af989e8870628f4a03ae1022d36f0910402040004000402040204000400050204020402040604000100040404020402010001010100040201000100d807d601b2a4730000d6028cb2db6308720173010001d6039372027302d604e4c6a70411d605e4c6a7050ed60695ef7203ed93c5b2a4730300c5a78fb2e4c6b2a57304000411730500b2e4c6720104117306007307d6079372027308d1ecec957203d80ad608b2a5dc0c1aa402a7730900d609e4c672080411d60adb63087208d60bb2720a730a00d60cdb6308a7d60db2720c730b00d60eb2720a730c00d60fb2720c730d00d6107e8c720f0206d611e4c6720104119683090193c17208c1a793c27208c2a793b27209730e009ab27204730f00731093e4c67208050e720593b27209731100b27204731200938c720b018c720d01938c720b028c720d02938c720e018c720f01937e8c720e02069a72109d9c7eb272117313000672107eb27211731400067315957206d801d608b2a5731600ed72079593c27208c2a7d801d609c67208050e95e67209ed93e472097205938cb2db6308b2a57317007318000172057319731a731b9595efec7206720393c5b2a4731c00c5a7731d7207731e',
    4,
    'EIP-004',
    2000000000000
);

insert into tokens (id, token_name, token_id, stake_token_id, stake_ergotree, decimals, token_type, emission_amount)
values (
    3,
    'EGIO',
    '00b1e236b60b95c2c6f8007a9d89bc460fc9e78f98b09faec9449007b40bccf3', -- token_id
    '1431964fa6559e969a7bf047405d3f63f7592354d432556f79894a12c4286e81', -- stake_token_id
    '1017040004000e20a8d633dee705ff90e3181013381455353dac2d91366952209ac6b3f9cdcc23e9040204000400040204020400040005020402040204060400040204040e20f419099a27aaa5f6f7d109d8773b1862e8d1857b44aa7d86395940d41eb5380604020402010001010100d802d601b2a4730000d6028cb2db6308720173010001959372027302d80bd603b2a5dc0c1aa402a7730300d604e4c672030411d605e4c6a70411d606db63087203d607b27206730400d608db6308a7d609b27208730500d60ab27206730600d60bb27208730700d60c8c720b02d60de4c672010411d19683090193c17203c1a793c27203c2a793b272047308009ab27205730900730a93e4c67203050ee4c6a7050e93b27204730b00b27205730c00938c7207018c720901938c7207028c720902938c720a018c720b01938c720a029a720c9d9cb2720d730d00720cb2720d730e00d801d603b2a4730f009593c57203c5a7d801d604b2a5731000d1ed93720273119593c27204c2a7d801d605c67204050e95e67205ed93e47205e4c6a7050e938cb2db6308b2a573120073130001e4c67203050e73147315d17316',
    4,
    'EIP-004',
    2000000000000
);

insert into tokens (id, token_name, token_id, stake_token_id, stake_ergotree, decimals, token_type, emission_amount)
values (
    4,
    'EXLE',
    '007fd64d1ee54d78dd269c8930a38286caa28d3f29d27cadcb796418ab15c283', -- token_id
    null, -- stake_token_id
    '100e04020400040404000402040604000402040204000400040404000400d810d601b2a4730000d602e4c6a7050ed603b2db6308a7730100d6048c720302d605e4c6a70411d6069d99db6903db6503feb27205730200b27205730300d607b27205730400d608b27205730500d6099972087204d60a9592720672079972087209999d9c7206720872077209d60b937204720ad60c95720bb2a5730600b2a5730700d60ddb6308720cd60eb2720d730800d60f8c720301d610b2a5730900d1eded96830201aedb63087201d901114d0e938c721101720293c5b2a4730a00c5a79683050193c2720cc2720193b1720d730b938cb2720d730c00017202938c720e01720f938c720e02720aec720bd801d611b2db63087210730d009683060193c17210c1a793c27210c2a7938c721101720f938c721102997204720a93e4c67210050e720293e4c6721004117205', -- vesting ergo tree
    4,
    'EIP-004',
    750000000000
);

insert into tokens (id, token_name, token_id, stake_token_id, stake_ergotree, decimals, token_type, emission_amount)
values (
    5,
    'Terahertz',
    '02f31739e2e4937bb9afb552943753d1e3e9cdd1a5e5661949cb0cef93f907ea', -- token_id
    null, -- stake_token_id
    '100e04020400040404000402040604000402040204000400040404000400d810d601b2a4730000d602e4c6a7050ed603b2db6308a7730100d6048c720302d605e4c6a70411d6069d99db6903db6503feb27205730200b27205730300d607b27205730400d608b27205730500d6099972087204d60a9592720672079972087209999d9c7206720872077209d60b937204720ad60c95720bb2a5730600b2a5730700d60ddb6308720cd60eb2720d730800d60f8c720301d610b2a5730900d1eded96830201aedb63087201d901114d0e938c721101720293c5b2a4730a00c5a79683050193c2720cc2720193b1720d730b938cb2720d730c00017202938c720e01720f938c720e02720aec720bd801d611b2db63087210730d009683060193c17210c1a793c27210c2a7938c721101720f938c721102997204720a93e4c67210050e720293e4c6721004117205', -- vesting ergo tree
    4,
    'EIP-004',
    400000000000
);

create extension hstore;

-- drop table utxos
create table utxos (
	id serial not null primary key,
	box_id varchar(64) not null unique,
	ergo_tree text,
	address varchar(64) not null,
	nergs bigint default 0, -- nergs
	registers hstore,
	assets hstore,
	transaction_id varchar(64),
	creation_height int,
	height int not null
);
create unique index on utxos (box_id);
```

# utxos
```sql-- truncate table utxos
insert into utxos (box_id, ergo_tree, address, nergs, registers, assets, transaction_id, creation_height, height)
    select 
        c.box_id::varchar(64)
        , c.ergo_tree::text
        , c.address::varchar(64)
        , c.nergs::bigint
        , trim(both '"' from c.registers)::hstore as registers
        , trim(both '"' from c.assets)::hstore as assets
        , trim(both '"' from c.transaction_id)::varchar(64) as transaction_id
        , c.creation_height::int
        , c.height::int
    from checkpoint_utxos c
```

# assets and balances (derived from utxos)
```sql
-- assets
-- drop table assets
create table assets (
	id serial not null primary key,
	address varchar(64),
	token_id varchar(64),
	amount bigint
);
create unique index on assets (address, token_id);
-- truncate table assets
-- insert into assets (address, token_id, amount)
with a as (
	select 
		address
		, (each(assets)).key::varchar(64) as token_id
		, (each(assets)).value::bigint as amount
	from utxos
	where address != '' -- only wallets; no smart contracts
)
-- truncate table assets
insert into assets (address, token_id, amount)
	select 
		address
		, token_id
		, sum(amount) as amount
	from a 
	group by address, token_id;
	
	
-- balances 
-- drop table balances
create table balances (
	id serial not null primary key,
	address varchar(64),
	nergs bigint
);
create unique index on balances (address);
-- truncate table balances
insert into balances (address, nergs)
	select address, sum(nergs) as nergs 
	from utxos
	where address != '' -- only wallets; no smart contracts
	group by address;

-- vesting
-- box_id, vesting_key_id, parameters, token_id, remaining, address, ergo_tree
-- drop table vesting
create table vesting (
	id serial not null primary key,
	box_id varchar(64),
	vesting_key_id varchar(64),
    parameters varchar(1024),
	token_id varchar(64),
    remaining bigint, -- nergs
	address varchar(64),
	ergo_tree text,
);
create unique index on vesting (address, token_id);

with v as (
	select id 
		, ergo_tree
		, box_id
		, (each(registers)).key::varchar(64) as register
		, right((each(registers)).value::text, length((each(registers)).value::text)-4) as token_id
		, (each(registers)).value::text as parameter
	from utxos
	where ergo_tree in (
			'100e04020400040404000402040604000402040204000400040404000400d810d601b2a4730000d602e4c6a7050ed603b2db6308a7730100d6048c720302d605e4c6a70411d6069d99db6903db6503feb27205730200b27205730300d607b27205730400d608b27205730500d6099972087204d60a9592720672079972087209999d9c7206720872077209d60b937204720ad60c95720bb2a5730600b2a5730700d60ddb6308720cd60eb2720d730800d60f8c720301d610b2a5730900d1eded96830201aedb63087201d901114d0e938c721101720293c5b2a4730a00c5a79683050193c2720cc2720193b1720d730b938cb2720d730c00017202938c720e01720f938c720e02720aec720bd801d611b2db63087210730d009683060193c17210c1a793c27210c2a7938c721101720f938c721102997204720a93e4c67210050e720293e4c6721004117205',
			'1012040204000404040004020406040c0408040a050004000402040204000400040404000400d812d601b2a4730000d602e4c6a7050ed603b2db6308a7730100d6048c720302d605db6903db6503fed606e4c6a70411d6079d997205b27206730200b27206730300d608b27206730400d609b27206730500d60a9972097204d60b95917205b272067306009d9c7209b27206730700b272067308007309d60c959272077208997209720a999a9d9c7207997209720b7208720b720ad60d937204720cd60e95720db2a5730a00b2a5730b00d60fdb6308720ed610b2720f730c00d6118c720301d612b2a5730d00d1eded96830201aedb63087201d901134d0e938c721301720293c5b2a4730e00c5a79683050193c2720ec2720193b1720f730f938cb2720f731000017202938c7210017211938c721002720cec720dd801d613b2db630872127311009683060193c17212c1a793c27212c2a7938c7213017211938c721302997204720c93e4c67212050e720293e4c6721204117206'
		)
)
, amounts as (
	select id, box_id, parameter, ergo_tree
	from v
	where register = 'R4'
)
, tokens as (
	select id, box_id, token_id
	from v
	where register = 'R5'
)
insert into vesting (address, token_id, parameter, ergo_tree, amount)
	select a.address, t.token_id, q.parameter, q.ergo_tree, a.amount
	from tokens t
		join amounts q on q.id = t.id
		join assets a on a.token_id = t.token_id
	-- where address in ('9eXGhU2T4SatNVHrPNt5KRExeQ9Jz89aamvx2Q7CHjHbnY1sRzG')
```

# store config for special keys
```sql
create table token_agg (
    id serial not null primary key,
    ergo_tree text,
    token_id varchar(64),
    height int,
    amount bigint,
    created_at timestamp default now()
)
insert into token_agg (ergo_tree, address, token_id, amount, notes)
values (
        '1017040004000e200549ea3374a36b7a22a803766af732e61798463c3332c5f6d86c8ab9195eed59040204000400040204020400040005020402040204060400040204040e2005cde13424a7972fbcd0b43fccbb5e501b1f75302175178fc86d8f243f3f312504020402010001010100d802d601b2a4730000d6028cb2db6308720173010001959372027302d80bd603b2a5dc0c1aa402a7730300d604e4c672030411d605e4c6a70411d606db63087203d607b27206730400d608db6308a7d609b27208730500d60ab27206730600d60bb27208730700d60c8c720b02d60de4c672010411d19683090193c17203c1a793c27203c2a793b272047308009ab27205730900730a93e4c67203050ee4c6a7050e93b27204730b00b27205730c00938c7207018c720901938c7207028c720902938c720a018c720b01938c720a029a720c9d9cb2720d730d00720cb2720d730e00d801d603b2a4730f009593c57203c5a7d801d604b2a5731000d1ed93720273119593c27204c2a7d801d605c67204050e95e67205ed93e47205e4c6a7050e938cb2db6308b2a573120073130001e4c67203050e73147315d17316',
        '3eiC8caSy3jiCxCmdsiFNFJ1Ykppmsmff2TEpSsXY1Ha7xbpB923Uv2midKVVkxL3CzGbSS2QURhbHMzP9b9rQUKapP1wpUQYPpH8UebbqVFHJYrSwM3zaNEkBkM9RjjPxHCeHtTnmoun7wzjajrikVFZiWurGTPqNnd1prXnASYh7fd9E2Limc2Zeux4UxjPsLc1i3F9gSjMeSJGZv3SNxrtV14dgPGB9mY1YdziKaaqDVV2Lgq3BJC9eH8a3kqu7kmDygFomy3DiM2hYkippsoAW6bYXL73JMx1tgr462C4d2PE7t83QmNMPzQrD826NZWM2c1kehWB6Y1twd5F9JzEs4Lmd2qJhjQgGg4yyaEG9irTC79pBeGUj98frZv1Aaj6xDmZvM22RtGX5eDBBu2C8GgJw3pUYr3fQuGZj7HKPXFVuk3pSTQRqkWtJvnpc4rfiPYYNpM5wkx6CPenQ39vsdeEi36mDL8Eww6XvyN4cQxzJFcSymATDbQZ1z8yqYSQeeDKF6qCM7ddPr5g5fUzcApepqFrGNg7MqGAs1euvLGHhRk7UoeEpofFfwp3Km5FABdzAsdFR9',
        'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413',
        0.0,
        'staked'
);

insert into token_agg (ergo_tree, address, token_id, amount, notes)
values (
        '10070400040204000500040004000400d80bd601e4c6a7040ed602b2db6308a7730000d6038c720201d604e4c6a70805d605e4c6a70705d606e4c6a70505d607e4c6a70605d6089c9d99db6903db6503fe720572067207d6098c720202d60a9972047209d60b958f99720472087207997204720a997208720ad1ed93b0b5a5d9010c63ededed93c2720c720193e4c6720c040ee4c6a7090e93b1db6308720c7301938cb2db6308720c7302000172037303d9010c41639a8c720c018cb2db63088c720c0273040002720bec937209720baea5d9010c63ededededededededed93c1720cc1a793c2720cc2a7938cb2db6308720c730500017203938cb2db6308720c73060002997209720b93e4c6720c040e720193e4c6720c0505720693e4c6720c0605720793e4c6720c0705720593e4c6720c0805720493e4c6720c090ee4c6a7090e',
        'Y2JDKcXN5zrz3NxpJqhGcJzgPRqQcmMhLqsX3TkkqMxQKK86Sh3hAZUuUweRZ97SLuCYLiB2duoEpYY2Zim3j5aJrDQcsvwyLG2ixLLzgMaWfBhTqxSbv1VgQQkVMKrA4Cx6AiyWJdeXSJA6UMmkGcxNCANbCw7dmrDS6KbnraTAJh6Qj6s9r56pWMeTXKWFxDQSnmB4oZ1o1y6eqyPgamRsoNuEjFBJtkTWKqYoF8FsvquvbzssZMpF6FhA1fkiH3n8oKpxARWRLjx2QwsL6W5hyydZ8VFK3SqYswFvRnCme5Ywi4GvhHeeukW4w1mhVx6sbAaJihWLHvsybRXLWToUXcqXfqYAGyVRJzD1rCeNa8kUb7KHRbzgynHCZR68Khi3G7urSunB9RPTp1EduL264YV5pmRLtoNnH9mf2hAkkmqwydi9LoULxrwsRvp',
        'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413',
        0.0,
        'vested'
);

insert into token_agg (ergo_tree, address, token_id, amount, notes)
values (
        '102d04000e20d71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de41304000e2005cde13424a7972fbcd0b43fccbb5e501b1f75302175178fc86d8f243f3f312504040404040604020400040204040402050004020400040004020402040404040400040004000402040004000e201028de73d018f0c9a374b71555c5b8f1390994f2f41633e7b9d68f77735782ee040004020100040604000500040204000400040204020400040204020404040404060100d802d601b2a4730000d602730195ed938cb2db6308720173020001730393c5b2a4730400c5a7d80ad603b2a5730500d604e4c672030411d605e4c672010411d606b27204730600d607b2a4730700d608b2e4c672070411730800d609db6308a7d60a9a8cb2db63087207730900029592b17209730a8cb27209730b0002730cd60bdb63087203d60cb2720b730d00d19683080193c27203c2a793b27204730e00b27205730f0093b27204731000b2720573110093b27204731200b27205731300937206958f7208720a7208720a938cb2720b731400018cb2720973150001938c720c017202938c720c0272069593c57201c5a7d80ad603b2a5731600d604db63087203d605db6308a7d6068cb2720573170002d607b5a4d9010763d801d609db630872079591b172097318ed938cb2720973190001731a93b2e4c672070411731b00b2e4c6a70411731c00731dd608e4c6a70411d609b27208731e00d60ab27208731f00d60bb072077320d9010b41639a8c720b019d9c8cb2db63088c720b02732100027209720ad60ce4c672030411d19683070193c27203c2a7938cb27204732200018cb272057323000195907206720b93b172047324d801d60db27204732500ed938c720d017202928c720d02997206720b93b2720c732600720a93b2720c732700b2720873280093b2720c73290099b27208732a007eb172070593b2720c732b007209d1732c',
        'xhRNa2Wo7xXeoEKbLcsW4gV1ggBwrCeXVkkjwMwYk4CVjHo95CLDHmomXirb8SVVtovXNPuqcs6hNMXdPPtT6nigbAqei9djAnpDKsAvhk5M4wwiKPf8d5sZFCMMGtthBzUruKumUW8WTLXtPupD5jBPELekR6yY4zHV4y21xtn7jjeqcb9M39RLRuFWFq2fGWbu5PQhFhUPCB5cbxBKWWxtNv8BQTeYj8bLw5vAH1WmRJ7Ln7SfD9RVePyvKdWGSkTFfVtg8dWuVzEjiXhUHVoeDcdPhGftMxWVPRZKRuMEmYbeaxLyccujuSZPPWSbnA2Uz6EketQgHxfnYhcLNnwNPaMETLKtvwZygfk1PuU9LZPbxNXNFgHuujfXGfQbgNwgd1hcC8utB6uZZRbxXAHmgMaWuoeSsni99idRHQFHTkmTKXx4TAx1kGKft1BjV6vcz1jGBJQyFBbQCTYBNcm9Yq2NbXmk5Vr7gHYbKbig7eMRT4oYxZdb9rwupphRGK4b2tYis9dXMT8m5EfFzxvAY9Thjbg8tZtWX7F5eaNzMKmZACZZqW3U7qS6aF8Jgiu2gdK12QKKBTdBfxaC6hBVtsxtQXYYjKzCmq1JuGP1brycwCfUmTUFkrfNDWBnrrmF2vrzZqL6WtUaSHzXzC4P4h346xnSvrtTTx7JGbrRCxhsaqTgxeCBMXgKgPGud2kNvgyKbjKnPvfhSCYnwhSdZYj8R1rr4TH5XjB3Wv8Z4jQjCkhAFGWJqVASZ3QXrFGFJzQrGLL1XX6cZsAP8cRHxqa7tJfKJzwcub7RjELPa2nnhhz5zj5F9MU1stJY4SBiX3oZJ6HdP9kNFGMR86Q6Z5qyfSRjwDNjVyvkKNoJ6Yk9nm367gznSVWkS9SG3kCUonbLgRt1Moq7o9CN5KrnyRgLrEAQU83SGY7Bc6FcLCZqQn8VqxP4e8R3vhf24nrzXVopydiYai',
        'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413',
        0.0,
        'emitted'
);

insert into token_agg (ergo_tree, address, token_id, amount, notes)
values (
        '1014040004000e2005cde13424a7972fbcd0b43fccbb5e501b1f75302175178fc86d8f243f3f312504020402040204040404040205000400040004000402040204000400040004000100d801d601b2a473000095ed938cb2db6308720173010001730293c5b2a4730300c5a7d808d602b2a5730400d603db63087202d604db6308a7d605b27204730500d606db6308b2a4730600d6079592b1720673078cb27206730800027309d6089a8c7205027207d609b2e4c6a70411730a00d19683050193c27202c2a7938cb27203730b00018cb27204730c0001959172087209d801d60ab27203730d00ed938c720a018c720501938c720a02997208720993b17203730e93b2e4c672020411730f00720993b2e4c6b2a57310000411731100999ab2e4c67201041173120072097207d17313',
        '9hXmgvzndtakdSAgJ92fQ8ZjuKirWAw8tyDuyJrXP6sKHVpCz8XbMANK3BVJ1k3WD6ovQKTCasjKL5WMncRB6V9HvmMnJ2WbxYYjtLFS9sifDNXJWugrNEgoVK887bR5oaLZA95yGkMeXVfanxpNDZYaXH9KpHCpC5ohDtaW1PF17b27559toGVCeCUNti7LXyXV8fWS1mVRuz2PhLq5mB7hg2bqn7CZtVM8ntbUJpjkHUc9cP1R8Gvbo1GqcNWgM7gZkr2Dp514BrFz1cXMkv7TYEqH3cdxX9c82hH6fdaf3n6avdtZ5bgqerUZVDDW6ZsqxrqTyTMQUUirRAi3odmMGmuMqDJbU3Z1VnCF9NBow7jrKUDSgckDZakFZNChsr5Kq1kQyNitYJUh9fra1jLHCQ9yekz3te9E',
        'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413',
        0.0,
        'stake_pool'
);

insert into token_agg (ergo_tree, address, token_id, amount, notes)
values (
    '100e04020400040404000402040604000402040204000400040404000400d810d601b2a4730000d602e4c6a7050ed603b2db6308a7730100d6048c720302d605e4c6a70411d6069d99db6903db6503feb27205730200b27205730300d607b27205730400d608b27205730500d6099972087204d60a9592720672079972087209999d9c7206720872077209d60b937204720ad60c95720bb2a5730600b2a5730700d60ddb6308720cd60eb2720d730800d60f8c720301d610b2a5730900d1eded96830201aedb63087201d901114d0e938c721101720293c5b2a4730a00c5a79683050193c2720cc2720193b1720d730b938cb2720d730c00017202938c720e01720f938c720e02720aec720bd801d611b2db63087210730d009683060193c17210c1a793c27210c2a7938c721101720f938c721102997204720a93e4c67210050e720293e4c6721004117205',
    '2k6J5ocjeESe4cuXP6rwwq55t6cUwiyqDzNdEFgnKhwnWhttnSShZb4LaMmqTndrog6MbdT8iJbnnwWEcNoeRfEqXBQW4ohBTgm8rDnu9WBBZSixjJoKPT4DStGSobBkoxS4HZMe4brCgujdnmnMBNf8s4cfGtJsxRqGwtLMvmP6Z6FAXw5pYveHRFDBZkhh6qbqoetEKX7ER2kJormhK266bPDQPmFCcsoYRdRiUJBtLoQ3fq4C6N2Mtb3Jab4yqjvjLB7JRTP82wzsXNNbjUsvgCc4wibpMc8MqJutkh7t6trkLmcaH12mAZBWiVhwHkCYCjPFcZZDbr7xeh29UDcwPQdApxHyrWTWHtNRvm9dpwMRjnG2niddbZU82Rpy33cMcN3cEYZajWgDnDKtrtpExC2MWSMCx5ky3t8C1CRtjQYX2yp3x6ZCRxG7vyV7UmfDHWgh9bvU',
    '1fd6e032e8476c4aa54c18c1a308dce83940e8f4a28f576440513ed7326ad489',
    0.0,
    'vested'
);

-- do this, or just don't manually assign values in above inserts
alter sequence tokens_id_seq restart with 6;
```

# tokens_tokenomics
```sql
create table tokens_tokenomics
(
    id serial not null primary key,
    address text,
    token_id varchar(64),
    box_id varchar(64),
    amount bigint,
    height integer
);
alter table tokens_tokenomics add unique (box_id, token_id);
create index tokens_tokenomics_boxid on tokens_tokenomics (box_id);
```

# store token_agg values from blockchain
```sql
create table tokens_tokenomics_agg (
    id serial not null primary key, 
    agg_id int, 
    token_id varchar(64),
    amount bigint,
    box_id varchar(64),
    height int
)
```

## PIT Tables
Create point-in-time tables to save reprocess time.  This is currently manual, but could be integrated pretty easily.
An issue with starting at a lower height is:
- tx A happens at height 10 - utxo box id is X
- tx B happens at height 20 - utxo box id is Y, X is now spent
- starting process at height 15, Y will be removed cause the height is lower than 20, but X will not exist.
- .. if catching up to current, this should not matter as it will be re-found at height 20
- .. if you are starting at a lower height, the reason may require the state match to point in time properly

```sql
-- drop PIT, boxes_XYZPDQ
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

create table prices(
	id serial not null primary key,
	token_id varchar(64) not null,
	price decimal(20, 10) default 0.0
)

create table tokens_alt
(
    id serial not null primary key,
    token_id varchar(64) NOT NULL,
    decimals integer,
    amount bigint,
    token_name varchar(1024),
    token_type varchar(64),
    token_price numeric(10,10) DEFAULT 0.0,
    height integer
);
```

# tokens
example: https://explorer.ergoplatform.com/en/transactions/08eaa2e5873a2b2edf29018d0f10577e08e737e7cae5496515b8938d350fe50e
when input boxId = output asset.token_id
- R4 = name 
- R5 = desc
- R6 = decimals
- R7 = type?? # 0101 - EIP-004?
- R8 = ??
use to find register values above: 
''.join([chr(r) for r in ErgoAppKit.deserializeLongArray('0e114572676f204e4654205475746f7269616c')]) # 'Ergo NFT Tutorial'
# TODO: 
invalid asset, {'tokenId': 'e86c662e2f508d3be8068738f307276db6dcf0054c10fe2a083c8da9e5db87c7', 'amount': 2000} at height 265850 while fetching tokens int() argument must be a string, a bytes-like object or a real number, not 'sigmastate.eval.CGroupElement'

## issue found between these tokens
token found: Charles Hoskinson 3D foto/0/8b0305aaea56cfb17cc4894c4ba5c150465a7f794ab5ac6e4ad0d3e03cd4060f/1
DEBUG:2022-07-08 13:32:25,077:ergopad:token found: Ergo anim/0/54b2264e33c91176d166141bb13d7dcde4f1dfa5d4c8cb39939991f6daebf110/1
ERROR:2022-07-08 13:32:25,131:ergopad:ERR: checkpointing tokens (psycopg2.errors.NumericValueOutOfRange) integer out of range

[SQL:
                insert into tokens_alt (token_id, height, amount, token_name, decimals)
                    select c.token_id, c.height, c.amount, c.token_name, c.decimals
                    from checkpoint_tokens_alt c
                    ;
            ]
(Background on this error at: https://sqlalche.me/e/14/9h9h)
DEBUG:2022-07-08 13:32:27,519:ergopad:token found: Female #03/0/5a7120a44aad2cd73f4d3a4819e1442c4c95a30876ef088be415285ae46c0bf6/1