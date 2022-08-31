create or replace view v_unspent_by_token as
	select distinct (each(assets)).key as token_id
		, box_id as box_id
	from utxos 
