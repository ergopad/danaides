create or replace view v_balances as
	select address, sum(nergs) as nergs 
	from utxos
	where address != '' -- only wallets; no smart contracts
	group by address
;