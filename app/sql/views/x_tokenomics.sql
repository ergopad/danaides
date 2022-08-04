-- need to deal with cascade reference if using dropNpop
create or replace view v_tokenomics as
    -- d71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413
    select token_name
        , k.token_id
        , k.token_price
        , k.supply_actual as current_total_supply
        , t.amount/power(10, t.decimals) as initial_total_supply
        , (t.amount - k.supply)/power(10, t.decimals) as burned
        , k.token_price * (supply - vested - emitted - stake_pool)/power(10, t.decimals) as market_cap
        , (supply - vested - emitted - stake_pool)/power(10, t.decimals) as in_circulation
    from tokenomics_ergopad k
        join tokens t on t.token_id = k.token_id

    -- 1fd6e032e8476c4aa54c18c1a308dce83940e8f4a28f576440513ed7326ad489
    union all
    select token_name
        , k.token_id
        , k.token_price
        , k.supply_actual as current_total_supply
        , t.amount/power(10, t.decimals) as initial_total_supply
        , (t.amount - k.supply)/power(10, t.decimals) as burned
        , k.token_price * (supply - vested - emitted - stake_pool)/power(10, t.decimals) as market_cap
        , (supply - vested - emitted - stake_pool)/power(10, t.decimals) as in_circulation
    from tokenomics_paideia k
        join tokens t on t.token_id = k.token_id
 
;