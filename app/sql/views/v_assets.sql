create view v_assets as 
	with a as (
		select 
			address
			, (each(assets)).key::varchar(64) as token_id
			, (each(assets)).value::bigint as amount
		from utxos
		where address != '' -- only wallets; no smart contracts
	)
	-- insert into {tbl} (address, token_id, amount)
	select 
		address
		, token_id
		, sum(amount) as amount
	from a 
	group by address, token_id
;