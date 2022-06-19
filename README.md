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
)
partition by list(is_unspent);

create table boxes_spent partition of boxes for values in (false);
create table boxes_unspent partition of boxes for values in (true);
