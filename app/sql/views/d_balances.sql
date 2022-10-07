create materialized view balances as
	select 
        address
        , sum(nergs) as nergs 
	from utxos
	where address != '' -- only wallets; no smart contracts
	group by address
	
    with no data;

create unique index uq_balances on balances (address);
