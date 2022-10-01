-- drop view v_token_staked cascade
create or replace view v_token_staked as
	select -- '[staked token name]' as project_name
		u.box_id
		-- , u.address
		, u.height
		, (u.assets->tid.token_id)::bigint as amount
		, (u.assets->tid.proxy_address)::int as proxy
		, u.registers->'R4' as penalty
		, regexp_replace(u.registers->'R5', '^0e20', '') as stakekey_token_id
		, t.decimals
		, t.token_id
	from utxos u
		join v_token_config tid on tid.stake_tree = u.ergo_tree
		join tokens t on t.token_id = tid.token_id
;
