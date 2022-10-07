create materialized view registers_by_token as
	with ep as (select box_id, skeys(assets) as token_id from utxos)
	select u.box_id
		, ep.token_id
		, assets->ep.token_id as amount
		, hstore_to_json(registers) as registers
		-- , t.decimals
		-- , t.token_price
		-- , t.token_name
	from utxos u
		join ep on ep.box_id = u.box_id
		-- left join tokens t on t.token_id = ep.token_id
	-- where ep.token_id = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'

    with no data;

create unique index uq_registers_by_token on registers_by_token (box_id, token_id);
