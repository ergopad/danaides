create view v_audit_stats as
	with ss as (
		select created_at
			, lag(created_at) over(order by created_at) as prev
			, height
			, service
		from audit_log
	)
	select max(height) as max_height
		, avg(created_at - prev) as avg_block
		, count(*) as block_count
		, service
	from ss
	where created_at is not null
		-- and service = 'boxes'
	group by service