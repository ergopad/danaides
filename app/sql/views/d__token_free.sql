create materialized view token_free as
	select address
		, a.amount/power(10, t.decimals) as amount
	from assets a
		join token_config tid on tid.token_id = a.token_id
		join tokens t on t.token_id = a.token_id
    
    -- with no data;
    with data;

create unique index uq_token_free on token_free (address);