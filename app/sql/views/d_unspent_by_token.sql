create materialized view unspent_by_token as
	select distinct (each(assets)).key as token_id
		, box_id as box_id
	from utxos
    
    with no data;

create unique index uq_unspent_by_token on unspent_by_token (token_id, box_id);
