create or replace view tokenomics as
	with k as (
		select vested
			, emitted
			, staked
			, stake_pool
			, 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413' as token_id
		from tokenomics_ergopad
	)
	select t.token_name
		, t.token_id
		, k.vested
		, k.emitted
		, k.staked
		, k.stake_pool
		, t.token_price
		, t.current_total_supply/power(10, t.decimals) as current_total_supply
		, t.emission_amount/power(10, t.decimals) as initial_total_supply
		, (t.emission_amount - t.current_total_supply)/power(10, t.decimals) as burned
		, t.token_price * (t.current_total_supply - k.vested - k.emitted - coalesce(k.stake_pool, 0))/power(10, t.decimals) as market_cap
		, (t.current_total_supply - k.vested - k.emitted - coalesce(k.stake_pool, 0))/power(10, t.decimals) as in_circulation
	from tokens t
		join k on k.token_id = t.token_id
	where t.token_id = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'
;