-- drop view v_token_locked cascade
create or replace view v_token_locked as
	with 
	adr as (
		select 
			address
			, id
			, (each(assets)).key::varchar(64) as token_id
			, (each(assets)).value::bigint as amount
		from utxos u
			-- join v_token_config tid on tid.stake_tree = u.ergo_tree
		where address != ''
	)
	select
		adr.address
		, u.token_id
		, u.box_id
		, u.stakekey_token_id
		, u.amount/power(10, u.decimals) as amount
		, u.penalty
	from v_token_staked u
		join adr on adr.token_id = u.stakekey_token_id
	where proxy = 1
;
-- drop table token_locked; select * into token_locked from v_token_locked
