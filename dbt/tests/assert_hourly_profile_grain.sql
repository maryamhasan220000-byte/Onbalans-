with expected as (
    select count(distinct(dutch_hour, is_weekend)) as combos
    from {{ref('fct_day_ahead_price')}}
), actual as(
    select count(*) as rows_present 
    from {{ref('agg_day_ahead_price_hourly')}}
)
 select * from expected, actual where combos <> rows_present 