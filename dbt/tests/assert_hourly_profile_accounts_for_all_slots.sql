--Every fact row must be pooled into exactly one profile row. 
-- Catches a filter in aggregate silently excluding slots.

with counts as (
    select 
         (select count(*) from {{ref('fct_day_ahead_price')}}) as fact_rows,
         (select sum(slots_observed) from {{ref('agg_day_ahead_price_hourly')}}) as pooled
)
select * from counts where fact_rows <> pooled