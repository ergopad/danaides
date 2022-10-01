-- drop view v_tokens_free cascade
create or replace view v_token_free as
	select address
		, a.amount/power(10, t.decimals) as amount
	from assets a
		join v_token_config tid on tid.token_id = a.token_id
		join tokens t on t.token_id = a.token_id
;
-- drop table tokens_free; select * into tokens_free from v_tokens_free
