create materialized view tokenomics_egio as
	with 
    assets as (
        select (each(assets)).key as token_id
            , (each(assets)).value::bigint as amount
            , ergo_tree
        from utxos
    )
	-- vested
	, vested as (
		select 0 as amount
		-- from assets
		-- where ergo_tree = ''
        --     and token_id = ''
	)
	-- emitted
	, emitted as (
		select 0 as amount
		-- from assets
		-- where ergo_tree = ''
	)
	-- staked
    , staked AS (
        select sum(coalesce(amount, 0)) as amount
        from assets
        where ergo_tree = '101b0400040004020e20097fd281c99588269d672e1b686bf6bcdce04102e183b2242f6634d93869fc0a04020e200e4202196f6030ab1986e39012dc95ea10f034b13d90a70c0b1f86d986106ff6040204000400040204000400050204020402040604000100040004000400040401000402040201010100d809d601b2a4730000d6028cb2db6308720173010001d603e4c6a70411d604e4c6a7050ed605db6308a7d606b27205730200d6077e8c72060206d6089372027303d60993c5b2a4730400c5a7d1ecec959372027305d807d60ab2a5dc0c1aa402a7730600d60be4c6720a0411d60cdb6308720ad60db2720c730700d60eb27205730800d60fb2720c730900d610e4c6720104119683090193c1720ac1a793c2720ac2a793b2720b730a009ab27203730b00730c93b2720b730d00b27203730e0093e4c6720a050e7204938c720d018c720e01938c720d028c720e02938c720f018c720601937e8c720f02069a72079d9c7eb27210730f000672077eb272107310000673119596830301720872098fb2e4c6b2a57312000411731300b2e4c672010411731400d801d60ab2a57315009593c2720ac2a7d801d60bc6720a050e9683020195e6720b93e4720b72047316938cb2db6308b2a57317007318000172047319731a9683020172087209'::text
            and token_id = '00b1e236b60b95c2c6f8007a9d89bc460fc9e78f98b09faec9449007b40bccf3'
    )
	-- stake pool
	, stake_pool as (
		select 0 as amount
		-- from assets 
		-- where ergo_tree = ''
        --     and token_id = ''
	)
    , supply as (
        -- leave as int (do not /decimals)
        select sum(coalesce(amount, 0)) as amount
        from assets
        where token_id = '00b1e236b60b95c2c6f8007a9d89bc460fc9e78f98b09faec9449007b40bccf3'
        group by token_id
    )
    , vals as (
		select
          (select coalesce(amount, 0) from vested) as vested -- 
        , (select coalesce(amount, 0) from staked) as staked -- 
        , (select coalesce(amount, 0) from emitted) as emitted -- 
        , (select coalesce(amount, 0) from stake_pool) as stake_pool -- 
        , (select coalesce(amount, 0) from supply) as supply -- 
    )
    select 
		'00b1e236b60b95c2c6f8007a9d89bc460fc9e78f98b09faec9449007b40bccf3' as token_id 
		, t.decimals
		, t.token_price 
		
        , vested as vested
        , staked as staked
        , emitted as emitted
        , stake_pool as stake_pool
        , supply as supply
		, supply - vested - emitted - stake_pool as in_circulation

		-- w/decimals
		, (vested/power(10, t.decimals))::decimal(32, 2) as vested_actual
        , (staked/power(10, t.decimals))::decimal(32, 2) as staked_actual
        , (emitted/power(10, t.decimals))::decimal(32, 2) as emitted_actual
        , (stake_pool/power(10, t.decimals))::decimal(32, 2) as stake_pool_actual
        , (supply/power(10, t.decimals))::decimal(32, 2) as supply_actual
		, ((supply - vested - emitted - stake_pool)/decimals)::decimal(32, 2) as in_circulation_actual
    from vals v
		join tokens t on t.token_id = '00b1e236b60b95c2c6f8007a9d89bc460fc9e78f98b09faec9449007b40bccf3'

    with no data;

create unique index uq_tokenomics_egio on tokenomics_egio (token_id);
